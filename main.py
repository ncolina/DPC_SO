from functions.interface_functions import interactive, commandline
import sys , time, os
import logging

logging.basicConfig(filename='logs/%s.log'%time.strftime('%Y-%m-%d-%H-%M-%S'),level=logging.DEBUG,format='%(levelname)s - %(message)s')
stderrLogger=logging.StreamHandler()
stderrLogger.setLevel(logging.INFO)
stderrLogger.setFormatter(logging.Formatter('%(message)s'))
logging.getLogger().addHandler(stderrLogger)
logging.debug('%s started the program', os.getlogin())
commandline()
