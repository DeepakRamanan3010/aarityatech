import ws from 'k6/ws';
import {check} from 'k6';

export let options = {
    stages: [
        {duration: '2m', target: 10},  // Ramp up to 10 VUs in 2 minutes
        {duration: '3m', target: 90},  // Stay at 90 VUs for 3 minutes
        {duration: '2m', target: 0},   // Ramp down to 0 VUs in 2 minutes
    ],
    thresholds: {
        'http_req_duration': ['p(95)<500'],  // Define your performance threshold here
    },
};

export default function () {
    const url = `ws://${__ENV.WS_HOST}/v1`;
    const params = {};

    const response = ws.connect(url, params,  (socket) => {
        socket.on('open', () => {});

        socket.on('ping', () => socket.ping());

        socket.on('error', (e) => {
            if (e.error() !== 'websocket: close sent') {
                console.log('An unexpected error occurred: ', e.error());
            }
        });

        socket.setTimeout( () => {
            socket.close();
        }, 2000);
    });
    check(response, {'status is 101': (r) => r && r.status === 101});
}
