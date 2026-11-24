"""Locust authenticated snapshot and bounded SSE/reconnect workload."""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from locust import HttpUser, task, between


class OperatorRead(HttpUser):
    wait_time = between(.2, .5)

    def on_start(self):
        self.cursor = None

    @property
    def headers(self):
        return {'Authorization': 'Bearer ' + Path(os.environ['GRID_LOAD_TOKEN_FILE']).read_text().strip()}

    @task(3)
    def snapshot(self):
        with self.client.get('/api/v1/overview', headers=self.headers, catch_response=True, timeout=5) as response:
            if response.status_code != 200:
                response.failure('snapshot status '+str(response.status_code))
            elif self.cursor is None:
                self.cursor = response.headers.get('X-Event-ID')

    @task
    def stream(self):
        start = time.monotonic()
        headers = dict(self.headers)
        if self.cursor:
            headers['Last-Event-ID'] = self.cursor
        first = True
        try:
            with self.client.get('/api/v1/events', headers=headers, stream=True, timeout=(3,20), catch_response=True, name='SSE connect') as response:
                if response.status_code != 200:
                    response.failure('stream status '+str(response.status_code))
                    return
                event_id = None
                for raw in response.iter_lines(chunk_size=1):
                    line = raw.decode()
                    if line.startswith('id: '):
                        event_id = line[4:]
                    if line.startswith('data: '):
                        value = json.loads(line[6:])
                        if event_id:
                            if self.cursor and event_id.split(':')[0] == self.cursor.split(':')[0] and int(event_id.split(':')[-1]) <= int(self.cursor.split(':')[-1]):
                                raise ValueError('non-increasing SSE cursor')
                            self.cursor = event_id
                        if first:
                            self.environment.events.request.fire(request_type='SSE',name='first event',response_time=(time.monotonic()-start)*1000,response_length=len(raw),exception=None)
                            first=False
                        if value.get('event_time'):
                            delay=(datetime.now(timezone.utc)-datetime.fromisoformat(value['event_time'].replace('Z','+00:00'))).total_seconds()*1000
                            self.environment.events.request.fire(request_type='SSE',name='event age',response_time=delay,response_length=len(raw),exception=None)
                    if time.monotonic()-start > 2:
                        break
            if first:
                self.environment.events.request.fire(request_type='SSE',name='heartbeat-only reconnect',response_time=(time.monotonic()-start)*1000,response_length=0,exception=None)
        except Exception as exc:
            self.environment.events.request.fire(request_type='SSE',name='disconnect/error',response_time=(time.monotonic()-start)*1000,response_length=0,exception=exc)
