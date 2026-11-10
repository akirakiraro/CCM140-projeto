import zmq

# Mesmo proxy da aula (Git Professor/pub-sub/src/proxy/proxy.py).
# 5555/5556 ja sao do broker Req-Rep; o enunciado pede XSUB 5557 e XPUB 5558.
context = zmq.Context()

pub = context.socket(zmq.XPUB)
pub.bind("tcp://*:5558")

sub = context.socket(zmq.XSUB)
sub.bind("tcp://*:5557")

print("proxy pronto: XSUB tcp://*:5557  XPUB tcp://*:5558", flush=True)
zmq.proxy(pub, sub)

pub.close()
sub.close()
context.close()
