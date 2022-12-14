import logging

def logger_creation(university):
    # Configuracion del logger
    logger = logging.getLogger(university)
    logger.setLevel('INFO')
    logPath = f'./dags/logs/{university}.log'
    fileHandler = logging.FileHandler(filename=logPath, delay=True)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s')
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)
    return logger