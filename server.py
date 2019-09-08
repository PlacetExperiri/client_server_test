import asyncio

dict_source = {}

def run_server(host, port):

    loop = asyncio.get_event_loop()
    coro = loop.create_server(ClientServerProtocol, host, port)

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()

def process_data(data_recieved):
    try:
        status, payload = data_recieved.split(" ", 1)
    except:
        return 'error\nwrong command\n\n'
    #print(f"status = {status}, payload = {payload}")
    
    if status == 'get':
        
        try:
            key = payload[:-1]
        except:
            return 'error\nwrong command\n\n'
        #print(f'get all values with the key {key}, len(key) = {len(key)}')
        
        if key:
            try:

                if str(key) == '*' and len(dict_source) > 0:
                    #print("get all the values")
                    keys = dict_source.keys()
                elif str(key) == '*' and len(dict_source) == 0:
                    #print('the dict is empty!')
                    return 'ok\n\n'
                elif key not in dict_source.keys():
                    #print("there is no such key!")
                    return 'ok\n\n'
                else:
                    keys = [key]

                resp = 'ok\n'
                
                #print(f"keys = {keys}")

                for each_key in keys:
                    #print(each_key)
                    for each_metrics in dict_source.get(each_key):
                        #print(each_metrics)
                        resp = resp + f"{each_key} {each_metrics[1]} {each_metrics[0]}\n"
                
                resp = resp + "\n" 

                return resp
            except:
                return 'error\nwrong command\n\n'
        else:
            return 'error\nwrong command\n\n'
    elif status == 'put':
        
        #print(f'payload = {payload}')
        try:
            key, value, timestamp = payload.split(' ')
        except:
            return 'error\nwrong command\n\n'
        
        #print(f'put value = {value}, timestamp = {timestamp}')
        
        if key and value and timestamp:
            try:
                
                if dict_source.get(key):
                    #print(f"to the key = {key} was added! value = {value}, time = {timestamp}")
                    if (int(timestamp), float(value)) not in dict_source[key]:
                        dict_source[key].append( (int(timestamp), float(value)) )
                        dict_source[key].sort(key=lambda x: x[0])
                    #print(dict_source)
                else:
                    #print(f"new key = {key} was added! value = {value}, time = {timestamp}")
                    dict_source.update({key: [(int(timestamp), float(value))]})
                    #print(dict_source)
                
                return 'ok\n\n'
            except:
                return 'error\nwrong command\n\n'
        else:
            return 'error\nwrong command\n\n'
    else:
        return 'error\nwrong command\n\n'


class ClientServerProtocol(asyncio.Protocol):
    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = process_data(data.decode())
        self.transport.write(resp.encode())

def _main():
    # проверка работы
    run_server(host = '127.0.0.1', port = 8888)

if __name__ == "__main__":
    _main()