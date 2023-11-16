#  -*- coding: utf-8 -*-
#
#  Copyright 2019 Shawn Seymour. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License"). You
#  may not use this file except in compliance with the License. A copy of
#  the License is located at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  or in the "license" file accompanying this file. This file is
#  distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
#  ANY KIND, either express or implied. See the License for the specific
#  language governing permissions and limitations under the License.

import logging
import signal
import sys
from functools import partial
from http.server import HTTPServer
from http.server import _get_best_family

from kafka_connect_healthcheck import health
from kafka_connect_healthcheck import helpers
from kafka_connect_healthcheck import parser
from kafka_connect_healthcheck.restarter import Restarter
from kafka_connect_healthcheck.handler import RequestHandler


def main():
    config_parser = parser.get_parser()
    args = config_parser.parse_args()

    logging.basicConfig(format="%(asctime)s.%(msecs)03d [%(levelname)7s] - %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        level=args.log_level)

    logging.info("Initializing healthcheck server...")

    restarter = Restarter(args.restart_connectors_regex, args.restart_error_regex, args.stop_restarting_after_seconds, args.initial_restart_delay_seconds)

    server_class = HTTPServer
    server_class.address_family, addr = _get_best_family('::', args.healthcheck_port)
    health_object = health.Health(args.connect_url, args.connect_worker_id, args.unhealthy_states.split(","),
                                  args.basic_auth, args.failure_threshold_percentage, args.considered_containers.split(","),
                                  restarter)
    handler = partial(RequestHandler, health_object)
    httpd = server_class(("", args.healthcheck_port), handler)
    host, port = httpd.socket.getsockname()[:2]
    url_host = f'[{host}]' if ':' in host else host
    logging.info(f"Serving HTTP on {host} port {port} (http://{url_host}:{port}/) ...")
    helpers.log_line_break()

    def stop(status_code, frame):
        logging.info("SIGINT/SIGTERM; exiting...")
        httpd.server_close()
        sys.exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        stop(0, None)


if __name__ == "__main__":
    main()
