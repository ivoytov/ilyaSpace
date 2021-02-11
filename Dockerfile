FROM python:3
ADD space.py /
ADD requirements.txt /
ADD starlink_historical_data.json /
RUN pip install -r requirements.txt 
CMD [ "python", "./space.py" ]
