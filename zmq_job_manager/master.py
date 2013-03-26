from zmq_helpers.socket_configs import EchoServer


class Master(EchoServer):
    pass


def parse_args():
    """Parses arguments, returns (options, args)."""
    from argparse import ArgumentParser

    parser = ArgumentParser(description='''Router-to dealer broker''')
    parser.add_argument(nargs=1, dest='rep_uri', type=str)
    args = parser.parse_args()
    args.rep_uri = args.rep_uri[0]
    return args


if __name__ == '__main__':
    args = parse_args()
    m = Master(args.rep_uri)
    m.run()
