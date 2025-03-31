import sqlite3

import structlog
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
from sqlalchemy.exc import DataError, IntegrityError, OperationalError, ProgrammingError

# 配置 structlog
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
)
logger = structlog.get_logger()


def exception_handler_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            logger.debug(
                f"Function {func.__name__} executed successfully",
                args=args,
                kwargs=kwargs,
            )
            return result
        except IntegrityError as e:
            logger.error(
                "SQLAlchemy IntegrityError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except ProgrammingError as e:
            logger.error(
                "SQLAlchemy ProgrammingError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except OperationalError as e:
            logger.error(
                "SQLAlchemy OperationalError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except DataError as e:
            logger.error(
                "SQLAlchemy DataError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except sqlite3.IntegrityError as e:
            logger.error(
                "SQLite IntegrityError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except sqlite3.ProgrammingError as e:
            logger.error(
                "SQLite ProgrammingError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except sqlite3.OperationalError as e:
            logger.error(
                "SQLite OperationalError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except FileNotFoundError as e:
            logger.error(
                "File I/O: File not found",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except PermissionError as e:
            logger.error(
                "File I/O: Permission denied",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except IsADirectoryError as e:
            logger.error(
                "File I/O: Expected a file, got a directory",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except ConnectionError as e:
            logger.error(
                "HTTP ConnectionError occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except Timeout as e:
            logger.error(
                "HTTP Timeout occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except TooManyRedirects as e:
            logger.error(
                "HTTP TooManyRedirects occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )
        except Exception as e:
            logger.error(
                "An unexpected error occurred",
                function=func.__name__,
                error=str(e),
                args=args,
                kwargs=kwargs,
            )

    return wrapper
