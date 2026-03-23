import socket
import logging
import signal
from .network import read_client_message, send_ack, read_exact, read_frame
from .protocol import deserialize_bet, deserialize_batch
from .utils import store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(5)
        self._client_sockets = []
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

        
        for client_sock in self._client_sockets:
            try: 
                client_sock.close()
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
                    self._client_sockets.append(client_sock)
                    self.__handle_client_connection(client_sock)
            except socket.timeout:
                self.shutdown_server(None, None)
                break
            except OSError as e:
                break


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
                    break

                if frame_type == 0x01:  # BATCH
                    bets, err = deserialize_batch(payload)
                    if err:
                        logging.error(f"action: apuesta_recibida | result: fail | cantidad: {len(bets)}")
                        # TODO: enviar NACK para que el cliente reenvie

                    if bets:
                        try:
                            store_bets(bets)
                            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
                        except Exception as e:
                            logging.error(f"action: store_bets | result: fail | error: {e}")
                            # TODO: enviar NACK para que el cliente reenvie
                            continue
                        try:
                            send_ack(client_sock)
                        except OSError as e:
                            logging.error(f"action: send_ack | result: fail | error: {e}")
                            break
                    else:
                        # empty batch -> ack to keep protocol deterministic
                        try:
                            send_ack(client_sock)
                        except OSError as e:
                            logging.error(f"action: send_ack | result: fail | error: {e}")
                            break
                    continue

                logging.error(f"action: receive_message | result: fail | error: invalid batch type {frame_type}")
                break

        finally:
            if client_sock in self._client_sockets:
                self._client_sockets.remove(client_sock)
            client_sock.close()

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
