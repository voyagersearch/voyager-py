import time
from voyager_tasks.utils import status


def execute(request=''):
    """TODO: add documentation string.
    :param request: json as a dict.
    """
    status_writer = status.Writer()
    status_writer.send_status(_('running pretend_task'))
    for i in range(0, 25):
        time.sleep(1)
        status_writer.send_percent(float(i)/25, _('working on item... ')+str(i), 'pretend_task')
    status_writer.send_status(_('finished pretend_task'))
