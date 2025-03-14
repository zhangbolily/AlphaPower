import model
from model.entity.alpha_simulate_result import *
from model._session import *

if __name__ == "__main__":
    Base.metadata.create_all(db_engine)