from sqlalchemy import (
    create_engine,
)

from sqlalchemy.orm import (
    sessionmaker,
    scoped_session,
)

db_engine = create_engine('sqlite:///test.db')
global_session = sessionmaker(bind=db_engine)
local_session = scoped_session(global_session)