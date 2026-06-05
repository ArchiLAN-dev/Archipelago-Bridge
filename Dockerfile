FROM python:3.10-slim

WORKDIR /service

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy repo contents into bridge/ subpackage so `python -m bridge.bridge` resolves
COPY . bridge/

ENV PYTHONPATH=/service

RUN useradd -m -u 1000 bridge && chown -R bridge:bridge /service
USER bridge

EXPOSE 5000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:5000/health')"

CMD ["python", "-m", "bridge.bridge"]
