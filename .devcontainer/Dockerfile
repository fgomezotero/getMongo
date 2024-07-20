FROM python:2.7

WORKDIR /usr/src/app

COPY . .

RUN /usr/local/bin/python -m pip install --upgrade pip \
    & /usr/local/bin/python -m pip install --no-cache-dir -r requirements/base.in

# CMD [ "python", "./your-daemon-or-script.py" ]
