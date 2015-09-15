from application.server import run
import threading


process_thread = threading.Thread(name='synchroniser', target=run)
process_thread.daemon = True
process_thread.start()
