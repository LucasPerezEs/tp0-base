import socket
import logging
import signal
from .network import read_client_message
from .protocol import deserialize_bet
from .utils import store_bets


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_socket.settimeout(1)
        self._client_sockets = []
        self._running = True


    def handle_sigterm(self, signum, frame):
        """
        Handle SIGTERM signal to gracefully shutdown the server

        When SIGTERM signal is received, the server socket is closed and the
        program exits
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

        signal.signal(signal.SIGTERM, self.handle_sigterm)

        while self._running:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    self._client_sockets.append(client_sock)
                    self.__handle_client_connection(client_sock)
            except socket.timeout:
                continue  # vuelve al while y chequea _running
            except OSError as e:
                break


    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            message = read_client_message(client_sock)
            bet = deserialize_bet(message)

            store_bets([bet])
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
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
