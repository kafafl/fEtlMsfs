import logging
import azure.functions as func
import datetime

from fxEtlMSFS import RunEtlMsfs

app = func.FunctionApp()

@app.schedule(schedule="*/20 * * * *", arg_name="myTimer", run_on_startup=True, use_monitor=False)

def fxTest(myTimer: func.TimerRequest) -> None:
    
    dtGetDate = datetime.datetime.now()
    if myTimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function executed.')
    
    RunEtlMsfs()
    


