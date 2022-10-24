# We use Meinheld (https://github.com/mopemope/meinheld) here, as it is the best WGSI server 
# This image does some auto optimizations (i.e. setting the right workers per core).
# Also, DO NOT use this image when an external processe manager (Like K8s or Docker Swarm is used)
FROM tiangolo/meinheld-gunicorn:python3.9
ENV STATIC_URL /static
ENV STATIC_PATH /var/www/app/static

# Copy and install requirements file
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Create App dir and copy app to it
COPY ./ /app