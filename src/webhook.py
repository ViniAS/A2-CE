from flask import Flask, request

from webhook_queue import store_user_behavior

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'POST':
        store_user_behavior.delay(request.data)
        return {'status': 'success'}, 200
    
    else:
        return {'status': 'error'}, 405

if __name__ == '__main__':
    app.run(host="0.0.0.0",port=50505)
