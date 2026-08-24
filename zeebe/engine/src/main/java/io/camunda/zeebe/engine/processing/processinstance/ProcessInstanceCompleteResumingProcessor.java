/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Camunda License 1.0. You may not use this file
 * except in compliance with the Camunda License 1.0.
 */
package io.camunda.zeebe.engine.processing.processinstance;

import io.camunda.zeebe.engine.processing.ExcludeAuthorizationCheck;
import io.camunda.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.camunda.zeebe.engine.processing.streamprocessor.SuspensionAware;
import io.camunda.zeebe.engine.processing.streamprocessor.SuspensionAware.SuspensionBehavior;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.SideEffectWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedRejectionWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.camunda.zeebe.engine.processing.timer.DueDateTimerCheckScheduler;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.ProcessMessageSubscriptionState;
import io.camunda.zeebe.engine.state.immutable.SuspensionState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.stream.api.records.TypedRecord;
import java.util.ArrayDeque;
import org.jspecify.annotations.NullMarked;

/**
 * Finalizes the resume lifecycle once the buffered-command drain is complete. Reacts to {@link
 * ProcessInstanceIntent#COMPLETE_RESUMING} by writing {@link ProcessInstanceIntent#RESUMED}.
 *
 * <p>Idempotent by design: writes {@code RESUMED} only when the instance exists, the suspension
 * marker is still {@link SuspensionState.State#RESUMING}, and the element is not mid-lifecycle-end
 * ({@code ELEMENT_TERMINATING}/{@code ELEMENT_COMPLETING}). Every other case is rejected rather
 * than silently skipped, so a concurrent drain chain restarted via {@code RESUME} (see {@link
 * ProcessInstanceResumeProcessor}) cannot write {@code RESUMED} twice.
 *
 * <p>{@link SuspensionBehavior#PROCESS} is unconditional: the marker is still {@code RESUMING} at
 * this point, and gating would strand the instance there forever.
 */
@ExcludeAuthorizationCheck
@NullMarked
public final class ProcessInstanceCompleteResumingProcessor
    implements TypedRecordProcessor<ProcessInstanceRecord>, SuspensionAware<ProcessInstanceRecord> {

  private static final String INSTANCE_GONE_MESSAGE =
      "Expected to finish resuming process instance '%d', but it no longer exists — likely "
          + "cancelled while resuming.";
  private static final String ALREADY_FINALIZED_MESSAGE =
      "Expected to finish resuming process instance '%d', but its suspension marker is no "
          + "longer RESUMING — likely already finalized by a concurrent resume.";
  private static final String LIFECYCLE_ENDING_MESSAGE =
      "Expected to finish resuming process instance '%d', but it is %s — resume is superseded "
          + "by the ending lifecycle.";

  private final StateWriter stateWriter;
  private final SideEffectWriter sideEffectWriter;
  private final TypedRejectionWriter rejectionWriter;
  private final ElementInstanceState elementInstanceState;
  private final SuspensionState suspensionState;
  private final DueDateTimerCheckScheduler timerChecker;
  private final ProcessMessageSubscriptionState processMessageSubscriptionState;
  private final SubscriptionCommandSender subscriptionCommandSender;

  public ProcessInstanceCompleteResumingProcessor(
      final ElementInstanceState elementInstanceState,
      final SuspensionState suspensionState,
      final Writers writers,
      final DueDateTimerCheckScheduler timerChecker,
      final ProcessMessageSubscriptionState processMessageSubscriptionState,
      final SubscriptionCommandSender subscriptionCommandSender) {
    stateWriter = writers.state();
    sideEffectWriter = writers.sideEffect();
    rejectionWriter = writers.rejection();
    this.elementInstanceState = elementInstanceState;
    this.suspensionState = suspensionState;
    this.timerChecker = timerChecker;
    this.processMessageSubscriptionState = processMessageSubscriptionState;
    this.subscriptionCommandSender = subscriptionCommandSender;
  }

  @Override
  public void processRecord(final TypedRecord<ProcessInstanceRecord> command) {
    final long processInstanceKey = command.getKey();
    final var elementInstance = elementInstanceState.getInstance(processInstanceKey);
    if (elementInstance == null) {
      reject(command, INSTANCE_GONE_MESSAGE.formatted(processInstanceKey));
      return;
    }

    if (suspensionState.getSuspensionState(processInstanceKey) != SuspensionState.State.RESUMING) {
      reject(command, ALREADY_FINALIZED_MESSAGE.formatted(processInstanceKey));
      return;
    }

    final var state = elementInstance.getState();
    if (state == ProcessInstanceIntent.ELEMENT_TERMINATING
        || state == ProcessInstanceIntent.ELEMENT_COMPLETING) {
      reject(command, LIFECYCLE_ENDING_MESSAGE.formatted(processInstanceKey, state));
      return;
    }

    reopenMessageSubscriptions(processInstanceKey);
    stateWriter.appendFollowUpEvent(
        processInstanceKey, ProcessInstanceIntent.RESUMED, elementInstance.getValue());
    // the instance is fully resumed now (the RESUMED applier clears the suspension marker), so
    // any timer that came due and was rejected while suspended can finally fire; nudge the
    // due-date checker rather than waiting for an unrelated future timer to wake it
    sideEffectWriter.appendSideEffect(
        () -> {
          timerChecker.scheduleTimer(-1);
          return true;
        });
  }

  @Override
  public SuspensionBehavior suspensionBehavior(final TypedRecord<ProcessInstanceRecord> record) {
    return SuspensionBehavior.PROCESS;
  }

  /**
   * Walks the element-instance tree BFS and re-opens message-side subscriptions for every {@code
   * OPENED} process message subscription. Subscriptions in {@code OPENING} or {@code CLOSING} state
   * are skipped: {@code OPENING} ones are mid-handshake (will complete normally), and {@code
   * CLOSING} ones are being torn down concurrently.
   *
   * <p>Sends {@link io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent#CREATE} to
   * the message partition, sourcing all 13 fields from the stored {@link
   * io.camunda.zeebe.protocol.impl.record.value.message.ProcessMessageSubscriptionRecord} (the
   * suspend path kept it {@code OPENED} as a durable manifest). The message partition's {@link
   * io.camunda.zeebe.engine.processing.message.MessageSubscriptionCreateProcessor} calls {@link
   * io.camunda.zeebe.engine.processing.message.MessageCorrelator#correlateNextMessage} on creation,
   * making TTL-correct pickup of buffered messages automatic. The ack-back {@code
   * PROCESS_MESSAGE_SUBSCRIPTION.CREATE} command arrives at the PI side and is rejected (the row is
   * {@code OPENED}, not {@code OPENING}) — the rejection is benign since the message-side
   * subscription and its correlation are the goal, not the PI-side state transition.
   */
  private void reopenMessageSubscriptions(final long processInstanceKey) {
    final var root = elementInstanceState.getInstance(processInstanceKey);
    if (root == null) {
      return;
    }
    final var queue = new ArrayDeque<ElementInstance>();
    queue.add(root);
    while (!queue.isEmpty()) {
      final var elementInstance = queue.poll();
      processMessageSubscriptionState.visitElementSubscriptions(
          elementInstance.getKey(),
          subscription -> {
            if (!subscription.isOpening() && !subscription.isClosing()) {
              final var record = subscription.getRecord();
              subscriptionCommandSender.openMessageSubscription(
                  record.getSubscriptionPartitionId(),
                  record.getProcessInstanceKey(),
                  record.getElementInstanceKey(),
                  record.getProcessDefinitionKey(),
                  record.getBpmnProcessIdBuffer(),
                  record.getMessageNameBuffer(),
                  record.getCorrelationKeyBuffer(),
                  record.isInterrupting(),
                  record.getTenantId(),
                  record.getBusinessIdBuffer(),
                  record.getElementIdBuffer(),
                  record.getRootProcessInstanceKey(),
                  record.getElementType());
            }
            return true;
          });
      elementInstanceState.getChildren(elementInstance.getKey()).stream()
          .filter(child -> child.getValue().getProcessInstanceKey() == processInstanceKey)
          .forEach(queue::add);
    }
  }

  private void reject(final TypedRecord<ProcessInstanceRecord> command, final String reason) {
    rejectionWriter.appendRejection(command, RejectionType.INVALID_STATE, reason);
  }
}
