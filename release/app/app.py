import mitzu.webapp.webapp as WA

from multiprocessing import set_start_method
import os


MULTIPROCESSING_START_METHOD = os.getenv("MULTIPROCESSING_START_METHOD", "forkserver")

set_start_method(MULTIPROCESSING_START_METHOD)


server = WA.create_dash_app().server
