import socket
import logging
import signal
import time
from .network import send_ack, read_frame, send_nack, read_ack
from .protocol import deserialize_batch, serialize_winners
from .utils import store_bets, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(5)
        self._client_sockets = {} # {agency_id: socket}
        self._clients_ready = 0
        self._expected_clients = listen_backlog
        self._running = True


    def shutdown_server(self, signum, frame):
        """
        Gracefully shutdown the server (can be used as a SIGTERM handler).
        """
        logging.info("action: shutdown | result: in_progress")
        
        self._running = False

        try:
            self._server_socket.close()
        except OSError as e:
            logging.error(f"action: closed server socket | result: fail | error: {e}")
        
        logging.info("action: closed server socket | result: success")


        for _agency_id, _client_sock in self._client_sockets.items():
            try: 
                _client_sock.close()
            except OSError as e:
                logging.error(f"action: closed client socket | result: fail | error: {e}")
            
            logging.info("action: closed client socket | result: success")
        
        logging.info("action: shutdown | result: success")
        

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communication
        finishes, servers starts to accept new connections again
        """

        signal.signal(signal.SIGTERM, self.shutdown_server)

        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    self.__handle_client_connection(client_sock)

                    if self._clients_ready >= self._expected_clients:
                        logging.info("action: sorteo | result: success")
                        self.process_bets()
                        self._running = False

            except socket.timeout:
                self.shutdown_server(None, None)
                break
            except OSError as e:
                break
        
        self.shutdown_server(None, None)


    def process_bets(self):
        bets = load_bets()
        winners_by_agency = {1: [], 2: [], 3: [], 4: [], 5: []}
        for bet in bets:
            if has_won(bet):
                winners_by_agency[bet.agency].append(bet)
                
        for agency_id, winners in winners_by_agency.items():
            for i in range(0,3):
                try:
                    message = serialize_winners(winners)
                except ValueError as e:
                    logging.error(f"action: serialize_winners | result: fail | error: {e}")
                    break

                if agency_id in self._client_sockets:
                    try:
                        self._client_sockets[agency_id].sendall(message)
                    except OSError as e:
                        logging.error(f"action: send_winners | result: fail | error: {e}")
                        continue
                
                # Wait for Client ACK
                try:
                    if read_ack(self._client_sockets[agency_id]):
                        break
                    else:
                        logging.error(f"action: read_ack | result: nack_received | agency_id: {agency_id}")
                        time.sleep(1)
                        continue
                except OSError as e:
                    logging.error(f"action: read_ack | result: fail | error: {e}")
                    break
        return
    

    def __handle_client_connection(self, client_sock):
        """
        High-level frame handling: uses network.read_frame and protocol.deserialize_batch
        """
        try:
            while True:
                try:
                    frame_type, payload = read_frame(client_sock)
                except OSError as e:
                    logging.error(f"action: receive_message | result: fail | error: {e}")
                    break

                if frame_type == 0x02:  # FIN
                    logging.info("action: received FIN frame | result: success")
                    try:
                        send_ack(client_sock)
                    except OSError as e:
                        logging.error(f"action: send_ack | result: fail | error: {e}")
                    
                    self._clients_ready += 1
                    return

                if frame_type == 0x01:  # BATCH
                    agency_id, bets, err = deserialize_batch(payload)
                    if err:
                        logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bets)}")
                        try:
                            send_nack(client_sock)
                        except OSError as e:
                            logging.error(f"action: send_nack | result: fail | error: {e}")
                            break
                        # continue waiting for the client to resend
                        continue
                    
                    # Save socket for later
                    if agency_id and agency_id not in self._client_sockets:
                        self._client_sockets[agency_id] = client_sock

                    if bets:
                        try:
                            store_bets(bets)
                            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
                        except Exception as e:
                            logging.error(f"action: store_bets | result: fail | error: {e}")
                            try:
                                send_nack(client_sock)
                            except OSError as e:
                                logging.error(f"action: send_nack | result: fail | error: {e}")
                                try:
                                    client_sock.close()
                                except OSError as e:
                                    pass
                                return
                            continue

                        try:
                            send_ack(client_sock)
                        except OSError as e:
                            logging.error(f"action: send_ack | result: fail | error: {e}")
                            try:
                                client_sock.close()
                            except OSError as e:
                                pass
                            return

                    else:
                        # empty batch -> ack to keep protocol deterministic
                        try:
                            send_ack(client_sock)
                        except OSError as e:
                            logging.error(f"action: send_ack | result: fail | error: {e}")
                            try:
                                client_sock.close()
                            except OSError as e:
                                pass
                            return
                    continue

                logging.error(f"action: receive_message | result: fail | error: invalid batch type {frame_type}")
                try:
                    client_sock.close()
                except OSError as e:
                    pass
                return

        finally:
            return

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
