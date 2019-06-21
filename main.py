import time
import zmq
import zmq.devices
from threading import Thread


def main():
    frontend_url = f'inproc://frontend'
    backend_url = f'inproc://backend'
    # frontend_url = f'ipc:///tmp/frontend'
    # backend_url = f'ipc:///tmp/backend'
    streamer = zmq.devices.ThreadDevice(zmq.STREAMER, zmq.PULL, zmq.PUSH)
    streamer.bind_in(frontend_url)
    streamer.bind_out(backend_url)
    streamer.setsockopt_in(zmq.IDENTITY, b'PULL')
    streamer.setsockopt_out(zmq.IDENTITY, b'PUSH')
    streamer.start()
    time.sleep(3)
    threads = [
        Thread(target=pusher, args=(frontend_url, 0)),
        Thread(target=pusher, args=(frontend_url, 1)),
        Thread(target=puller, args=(backend_url, 2)),
        Thread(target=puller, args=(backend_url, 3))
    ]
    for t in threads:
        t.daemon = True
        t.start()
        # print(t)
    while True:
        time.sleep(5)


def pusher(sock_url, tid):
    context = zmq.Context.instance()
    sock = context.socket(zmq.PUSH)
    sock.connect(sock_url)
    while True:
        # print(f'Pusher {tid} pushing..')
        sock.send_json({'sup': 'nerds'})
        time.sleep(1)


def puller(sock_url, tid):
    context = zmq.Context.instance()
    sock = context.socket(zmq.PULL)  # Socket for sending replies to index_runner
    sock.connect(sock_url)
    while True:
        data = sock.recv_json()
        print(f'Puller {tid} received: {data}')
        time.sleep(1)


if __name__ == '__main__':
    main()
