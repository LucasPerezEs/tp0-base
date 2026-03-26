import socket
import logging
import signal
import time
import threading
from .network import send_ack, read_frame, send_nack, read_ack
from .protocol import deserialize_batch, serialize_winners
from .utils import store_bets, load_bets, has_won


class Server:
    def __init__(self, port, listen_backlog, expected_clients):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._client_sockets = {} # {agency_id: socket}
        self._expected_clients = expected_clients
        self._running = True
        # Concurrency
        self._lock = threading.Lock()
        self._barrier = threading.Barrier(expected_clients)
        self._threads = []


    def shutdown_server(self, signum, frame):
        """
        Gracefully shutdown the server (can be used as a SIGTERM handler).
        """
        logging.info("action: shutdown | result: in_progress")
        
        self._running = False

        try:
            self._barrier.abort()
        except Exception as e:
            logging.error(f"action: barrier_abort | result: fail | error: {e}")

        try:
            self._server_socket.close()
        except OSError as e:
            logging.error(f"action: closed server socket | result: fail | error: {e}")
        
        logging.info("action: closed server socket | result: success")

        with self._lock:
            sockets_snapshot = list(self._client_sockets.items())
            self._client_sockets.clear() 

        for _agency_id, _client_sock in sockets_snapshot:
            try:
                _client_sock.close()
                logging.info("action: closed client socket | result: success")
            except OSError as e:
                logging.error(f"action: closed client socket | result: fail | error: {e}")


        self._wait_threads(timeout=5)
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
                    t = threading.Thread(target=self.__handle_client_connection, args=(client_sock,), daemon=False)
                    t.start()
                    self._threads.append(t)

            except socket.timeout:
                logging.debug("action: accept_connections | result: timeout")
                break
            except OSError as e:
                break
        
        self.shutdown_server(None, None)
        self._wait_threads()

    
    def _wait_threads(self, timeout=None):
        for t in self._threads:
            try:
                if timeout is None:
                    t.join()
                else:
                    t.join(timeout)
            except RuntimeError as e:
                logging.error(f"action: wait_threads | result: fail | error: {e}")
                continue

    def process_bets(self, agency_id, client_sock):
        with self._lock:
            bets = load_bets()

        winners = [b for b in bets if b.agency == agency_id and has_won(b)]

        try:
            message = serialize_winners(winners)
        except ValueError as e:
            logging.error(f"action: serialize_winners | result: fail | error: {e}")
            with self._lock:
                self._client_sockets.pop(agency_id, None)
            try:
                client_sock.close()
            except OSError as e:
                pass
            return

        for i in range(3):

            try:
                client_sock.sendall(message)
            except OSError as e:
                logging.error(f"action: send_winners | result: fail | error: {e}")
                break
            
            # Wait for Client ACK
            try:
                if read_ack(client_sock):
                    break
                else:
                    logging.error(f"action: read_ack | result: nack_received | agency_id: {agency_id}")
                    time.sleep(1)
                    continue
            except OSError as e:
                logging.error(f"action: read_ack | result: fail | error: {e}")
                break

        return
    

    def _send_ack(self, sock) -> bool:
        try:
            send_ack(sock)
            return True
        except OSError as e:
            logging.error(f"action: send_ack | result: fail | error: {e}")
            return False

    def _send_nack(self, sock) -> bool:
        try:
            send_nack(sock)
            return True
        except OSError as e:
            logging.error(f"action: send_nack | result: fail | error: {e}")
            return False


    def _register_socket(self, agency_id: int, sock) -> None:
        if not agency_id:
            return
        # store socket for later results if not present
        with self._lock:
            if agency_id and agency_id not in self._client_sockets:
                self._client_sockets[agency_id] = sock


    def _handle_fin(self, client_sock) -> bool:
        """Handle FIN frame. Return True to continue server loop, False to stop handler."""
        if not self._send_ack(client_sock):
            return False
        
        try:
            # wait until all clients have sent FIN
            self._barrier.wait()
        except threading.BrokenBarrierError:
            logging.error("action: barrier_wait | result: fail | error: BrokenBarrierError")
            return False
        
        # find agency_id associated with this socket
        with self._lock:
            agency_id = None
            for aid, s in self._client_sockets.items():
                if s is client_sock:
                    agency_id = aid
                    break

        if agency_id is None:
            logging.warning("action: send_winners | result: skip | reason: unknown agency for socket")
            return False
        
        self.process_bets(agency_id, client_sock)

        with self._lock:
            self._client_sockets.pop(agency_id, None)

        return False  # after FIN, handler should stop



    def _handle_batch(self, payload: bytes, client_sock) -> bool:
        """Process batch payload. Return True to continue loop, False to stop handler."""
        agency_id, bets, err = deserialize_batch(payload)
        if err:
            logging.error(f"action: apuesta_recibida | result: fail |  err:{err}")
            if not self._send_nack(client_sock):
                return False
            return True  # wait for resend

        # register socket for results
        self._register_socket(agency_id, client_sock)

        if bets:
            try:
                with self._lock:
                    store_bets(bets)
                logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(bets)}")
            except Exception as e:
                logging.error(f"action: store_bets | result: fail | error: {e}")
                if not self._send_nack(client_sock):
                    return False
                return True  # client should resend
            if not self._send_ack(client_sock):
                return False
        else:
            # empty batch -> ack
            if not self._send_ack(client_sock):
                return False
        return True


    def __handle_client_connection(self, client_sock):
        """
        High-level frame handling: uses network.read_frame
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
                    if not self._handle_fin(client_sock):
                        break
                    return

                elif frame_type == 0x01:  # BATCH
                    cont = self._handle_batch(payload, client_sock)
                    if not cont:
                        break
                    # continue waiting for batches or FIN from same client
                    continue

                else:
                    logging.error(f"action: receive_message | result: fail | error: invalid batch type {frame_type}")
                    break

        finally:
            try:
                client_sock.close()
            except OSError as e:
                logging.error(f"action: closed client socket | result: fail | error: {e}")
            logging.info("action: closed client socket | result: success")
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
