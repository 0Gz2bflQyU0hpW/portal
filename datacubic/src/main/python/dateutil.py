import time
import datetime

def subday(rn,type):
    today=datetime.date.today()
    rnday=datetime.timedelta(days=int(rn))
    myday=today-rnday

    if type == 0:
	date = str(myday)+' 00:00:00.00'
	return long(time.mktime(time.strptime(date,'%Y-%m-%d %H:%M:%S.%f'))* 1000)
    if type == 1:
        return myday.strftime('%Y%m%d')

