import ws from 'k6/ws';
import { check } from 'k6';
import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

/*
    This test will connect to the websocket server and wait for 5 seconds
    before closing the connection. Only checks the connection upgrade status.
*/

const ids = [10854]
// for (let i = 0; i < 1000; i++) {
//     ids.push(randomIntBetween(1, 1000000))
// }

export default function () {
    const url = `ws://${__ENV.WS_HOST}/v1`;

    const res = ws.connect(url, {}, function (socket) {
        socket.on("open", () => {
            socket.send(JSON.stringify({
                "f": [
                    {
                        "m": 2,
                        "i": ids,
                    }
                ]
            }))
        })

        socket.on('error', function (e) {
            if (e.error() != 'websocket: close sent') {
                console.log('An unexpected error occured: ', e.error());
            }
        });

        socket.setTimeout(() => {
            socket.close()
        }, 10000)
    });

    if(res.status !== 101) console.log(res.error);

    check(res, { 'status is 101': (r) => r && r.status === 101 });
}
