package com.netflix.conductor.core.execution;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.dao.ExecutionDAO;
import com.netflix.conductor.dao.QueueDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * A helper service that tries to keep ExecutionDAO and QueueDAO in sync, based on the
 * task or workflow state.
 */
public class WorkflowRepairService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowRepairService.class);

    private final ExecutionDAO executionDAO;
    private final QueueDAO queueDAO;

    @Inject
    public WorkflowRepairService(
            ExecutionDAO executionDAO,
            QueueDAO queueDAO
    ) {
        this.executionDAO = executionDAO;
        this.queueDAO = queueDAO;
    }

    public void verifyAndRepairWorkflow(String workflowId, boolean includeTasks) {
        Workflow workflow = executionDAO.getWorkflow(workflowId, includeTasks);
        verifyAndRepairWorkflow(workflow);
        if (includeTasks) {
            workflow.getTasks().forEach(task -> verifyAndRepairTask(task));
        }
    }

    private boolean verifyAndRepairWorkflow(Workflow workflow) {
        if (workflow.getStatus().equals(Workflow.WorkflowStatus.RUNNING)) {
            if (!queueDAO.containsMessage("_deciderQueue", workflow.getWorkflowId())) {
                // TODO verify offsetTimeInSecond
                queueDAO.push("_deciderQueue", workflow.getWorkflowId(), 30);
                return true;
            }
        }
        return true;
    }

    /**
     * Verify if ExecutionDAO and QueueDAO agree for the provided task.
     * @param task
     * @return
     */
    private boolean verifyAndRepairTask(Task task) {
        if (task.getStatus().equals(Task.Status.SCHEDULED)) {
            // Ensure QueueDAO contains this taskId
            if (!queueDAO.containsMessage(task.getTaskDefName(), task.getTaskId())) {
                // TODO verify offsetTimeInSecond
                queueDAO.push(task.getTaskDefName(), task.getTaskId(), 0);
                return true;
            }
        }
        return false;
    }
}
