#app.py

# importing Flask and other modules
from flask import Flask, flash, request, redirect, url_for, render_template
import os
import json
import requests
from werkzeug.utils import secure_filename
 
# Flask constructor
app = Flask(__name__)
 

UPLOAD_FOLDER = 'static/uploads/'
 
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
 
ALLOWED_EXTENSIONS = set(['png', 'jpg', 'jpeg', 'gif'])
 
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS
     
 
@app.route('/')
def home():
    return render_template('index.html')
 
@app.route('/', methods=['POST'])
def upload_image():
    if 'file' not in request.files:
        flash('No file part')
        return redirect(request.url)
    file = request.files['file']
    if file.filename == '':
        flash('No image selected for uploading')
        return redirect(request.url)
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        #print('upload_image filename: ' + filename)
        flash('Image successfully uploaded and displayed below')
        return render_template('index.html', filename=filename)
    else:
        flash('Allowed image types are - png, jpg, jpeg, gif')
        return redirect(request.url)
 
@app.route('/display/<filename>')
def display_image(filename):
    #print('display_image filename: ' + filename)
    return redirect(url_for('static', filename='uploads/' + filename), code=301)
 
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)


# A decorator used to tell the application
# which URL is associated function
# Only allow post requests to this url
@app.route('/predictsmile', methods=["POST",])
def predict_smile():
    if request.method == "POST":
        prediction_input = [
            {
                "ntp": int(request.form.get("ntp")),  # getting input with name = ntp in HTML form
                "pgc": int(request.form.get("pgc")),  # getting input with name = pgc in HTML form
                "dbp": int(request.form.get("dbp")),
                "tsft": int(request.form.get("tsft")),
                "si": int(request.form.get("si")),
                "bmi": float(request.form.get("bmi")),
                "dpf": float(request.form.get("dpf")),
                "age": int(request.form.get("age"))
            }
        ]
        print(prediction_input)
        # use requests library to execute the prediction service API by sending a HTTP POST request
        # localhost or 127.0.0.1 is used when the applications are on the same machine.
        res = requests.post('http://localhost:5000/diabetes_predictor/', json=json.loads(json.dumps(prediction_input)))
        print(res.status_code)
        result = res.json()
        return result



