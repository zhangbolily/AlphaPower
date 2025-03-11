import model
from model.entity.alpha_simulate_result import *
from model.session import *

if __name__ == "__main__":
    Base.metadata.create_all(db_engine)