from interface.objects import ProcessStateEnum, Process

EE_STATE_RUNNING = "500-RUNNING"
EE_STATE_TERMINATED = "700-TERMINATED"
EE_STATE_UNKNOWN = "900-UNKNOWN"


_PD_PROCESS_STATE_MAP = {
    "100-UNSCHEDULED": ProcessStateEnum.REQUESTED,
    "150-UNSCHEDULED_PENDING": ProcessStateEnum.REQUESTED,
    "200-REQUESTED": ProcessStateEnum.REQUESTED,
    "250-DIED_REQUESTED": ProcessStateEnum.REQUESTED,
    "300-WAITING": ProcessStateEnum.WAITING,
    "350-ASSIGNED": ProcessStateEnum.PENDING,
    "400-PENDING": ProcessStateEnum.PENDING,
    "500-RUNNING": ProcessStateEnum.RUNNING,
    "600-TERMINATING": ProcessStateEnum.TERMINATING,
    "700-TERMINATED": ProcessStateEnum.TERMINATED,
    "800-EXITED": ProcessStateEnum.EXITED,
    "850-FAILED": ProcessStateEnum.FAILED,
    "900-REJECTED": ProcessStateEnum.REJECTED
}

_PD_PYON_PROCESS_STATE_MAP = {
    ProcessStateEnum.REQUESTED: "200-REQUESTED",
    ProcessStateEnum.WAITING: "300-WAITING",
    ProcessStateEnum.PENDING: "400-PENDING",
    ProcessStateEnum.RUNNING: "500-RUNNING",
    ProcessStateEnum.TERMINATING: "600-TERMINATING",
    ProcessStateEnum.TERMINATED: "700-TERMINATED",
    ProcessStateEnum.EXITED: "800-EXITED",
    ProcessStateEnum.FAILED: "850-FAILED",
    ProcessStateEnum.REJECTED: "900-REJECTED"
}
