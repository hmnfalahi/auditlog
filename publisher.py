from audit_log_service import RMQService


service = RMQService()


for i in range(10):
    service.sender(message='this is message: {0}'.format(i), delay=5000) 

