FROM prefecthq/prefect:2-latest
COPY requirements.txt /opt/prefect/104/requirements.txt
RUN python -m pip install -r requirements.txt
COPY . /opt/prefect/pacc-2024/
WORKDIR /opt/prefect/pacc-2024/

