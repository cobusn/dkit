FROM python:3.11-slim

# Update OS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      python3.11-venv \
      vim \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# use venv for python package installation 
RUN python3.11 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY  . /app
WORKDIR /app
RUN pip install --upgrade pip cython wheel cffi && \
    pip install -r requirements.txt && \
    pip install . && \
    rm -rf * 

# CMD ["python", "your_app.py"]
