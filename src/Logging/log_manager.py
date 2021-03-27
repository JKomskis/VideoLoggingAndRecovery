class LogManager():
    def __init__(self):
        # setup log file if needed
        pass

    # each log record should include txn_id, offset of last log record for this txn,
    # type of record, and length of record so we can quickly seek over it
    def log_begin_txn_record(txn_id: int) -> None:
        pass

    def log_update_record(txn_id: int) -> None:
        # write log record that includes txn id, name of video updated, path to before image, and path to after image
        pass

    def log_commit_txn_record(txn_id: int) -> None:
        pass

    def log_abort_txn_record(txn_id: int) -> None:
        pass

    def rollback_txn(txn_id: int) -> None:
        # read log file and undo txn's changes
        pass

    def recover_log() -> None:
        # ARIES style three phase recovery protocol
        # Analysis

        # Undo

        # Redo
        pass