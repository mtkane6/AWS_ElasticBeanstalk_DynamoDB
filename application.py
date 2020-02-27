from flask import Flask, request, render_template
import urllib.request
# import boto3

# print a nice greeting.
# def say_hello(username = "World"):
#     return '<p>Hello %s!</p>\n' % username


# some bits of text for the page.
# header_text = '''
#     <html>\n<head> <title>EB Flask Test</title> </head>\n<body>'''
# instructions = '''
#     <p><em>Hint</em>: This is a RESTful web service! Append a username
#     to the URL (for example: <code>/Thelonious</code>) to say hello to
#     someone specific.</p>\n'''
# home_link = '<p><a href="/">Back</a></p>\n'
# footer_text = '</body>\n</html>'

# EB looks for an 'application' callable by default.
application = Flask(__name__)

# add a rule for the index page.
# application.add_url_rule('/', 'index', (lambda: header_text +
#     say_hello() + instructions + footer_text))

# add a rule when the page is accessed with a name appended to the site
# URL.
# application.add_url_rule('/<username>', 'hello', (lambda username:
    # header_text + say_hello(username) + home_link + footer_text))


@application.route('/')
def HomePage():
    return render_template('index.html')

@application.route('/query/', methods=['POST'])
def QueryName():
    lastName = request.form['last']
    firstName = request.form['first']
    message = firstName + ", " + lastName
    return render_template('index.html', queryMessage = message)

@application.route('/load/', methods=['POST'])
def LoadData():
    target_url = "https://s3-us-west-2.amazonaws.com/css490/input.txt"
    
    headers = {}
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17'

    request = urllib.request.Request(target_url, headers=headers)
    resp = urllib.request.urlopen(request)
    respData = resp.read()

    # print(respData)
    # message = "Loaded data."
    return render_template('index.html', loadMessage=respData)

@application.route('/delete/', methods=['POST'])
def DeleteData():
    message = "Deleted data."
    return render_template('index.html', deleteMessage=message)


# run the app.
if __name__ == "__main__":
    # Setting debug to True enables debug output. This line should be
    # removed before deploying a production app.
    application.debug = True
    application.run()