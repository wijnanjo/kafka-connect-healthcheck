import re, logging
from tenacity.wait import wait_exponential
from tenacity import RetryCallState
from datetime import datetime
import time


class Restarter():
    def __init__(self, 
                restart_connectors_regex: str, restart_error_regex: str, \
                stop_restarting_after_seconds: int, initial_restart_delay_seconds: int) -> None:
        self.restart_connectors_regex = restart_connectors_regex or ".*"
        self.restart_error_regex = restart_error_regex or ".*"
        self.stop_restarting_after_seconds = stop_restarting_after_seconds
        self.initial_restart_delay_seconds = initial_restart_delay_seconds
        self.retryingtasks = {}
        logging.info("Task restarter configuration:")
        logging.info(f"\trestart_connectors_regex: {self.restart_connectors_regex}")
        logging.info(f"\trestart_error_regex: {self.restart_error_regex}")
        logging.info(f"\tstop_restarting_after_seconds: {self.stop_restarting_after_seconds}")
        logging.info(f"\trestart_erroinitial_restart_delay_secondsr_regex: {self.initial_restart_delay_seconds}")
        self.dryrun_retry_periods()

    def dryrun_retry_periods(self):
        waiter = wait_exponential(exp_base=1.6, multiplier=5, min=self.initial_restart_delay_seconds, max=self.stop_restarting_after_seconds)
        retrystate = RetryCallState(None, None,None,None)
        for _ in range(20):
            logging.debug(f"attempt {retrystate.attempt_number} on {waiter(retrystate)}")
            retrystate.attempt_number+=1

    def _get_task_name(self, task, connector):
        return f'{connector["name"]}_{task["id"]}'

    def mark_task_healthy(self, task, connector) -> None:        
        name = self._get_task_name(task, connector)
        if name in self.retryingtasks:
            del self.retryingtasks[name]

    def should_restart_task(self, task, connector) -> bool:
        name = self._get_task_name(task, connector)

        # only consider failed tasks
        if task["state"] != "FAILED":
            return False
        if not self.is_task_restartable(task, connector):
            logging.info(f"Task '{name}' is failed but it is not restartable")
            return False        
        
        state = self.retryingtasks.get(name, None)
        if state is None:
            state = TaskRestartState(self.initial_restart_delay_seconds, self.stop_restarting_after_seconds)
            self.retryingtasks[name]=state
        
        result = state.should_restart(task, connector)
        if result == RestartResult.TOO_EARLY:
            return False
        if result == RestartResult.TOO_LATE:
            return False
        else:
            return True            

    def is_task_restartable(self, task, connector) -> bool:
        return task['state'] == "FAILED" \
            and re.search(self.restart_connectors_regex, connector["name"]) is not None \
            and re.search(self.restart_error_regex, task["trace"]) is not None


class RestartResult:
    # a restart was triggered
    DO_RESTART = 0
    # waiting for (backoff) period to elapse
    TOO_EARLY = 1
    # retried long enough, we should stop
    TOO_LATE = 3


class TaskRestartState:    
    def __init__(self, initial_delay, max_duration) -> None:
        self.waiter = wait_exponential(min=initial_delay, max=max_duration)
        self.retrystate = RetryCallState(None, None,None,None)
        self.retrystate.outcome_timestamp
        # logging.debug(f"attempt {self.retrystate.attempt_number} - next {next} - min {min} - max {max}")

    def should_restart(self, task, connector) -> RestartResult:        
        now = time.monotonic()
        min = self.retrystate.start_time + self.waiter.min
        max = self.retrystate.start_time + self.waiter.max
        next = self.retrystate.start_time + self.waiter(self.retrystate)        
        if now < min or now < next:
            logging.info("Connector '{}' task '{}' awaiting end of restart backoff period ({}s)".format(connector["name"], task["id"], round(next-now, 1)))
            return RestartResult.TOO_EARLY
        if now > max:
            logging.info("Connector '{}' task '{}' will not restart anymore".format(connector["name"], task["id"]))
            return RestartResult.TOO_LATE
        if now >= next:
            logging.info("Connector '{}' task '{}' will be restarted now".format(connector["name"], task["id"]))
            self.retrystate.attempt_number+=1
            return RestartResult.DO_RESTART
