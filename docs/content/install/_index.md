+++
title = "Installation"
weight = 20
+++

{{% notice note %}}
Note: check out the [tutorial]({{< relref "tutorial/_index.md" >}}) if you want a first impression of zrepl.
{{% /notice %}}

## User Privileges

It is possible to run zrepl as an unprivileged user in combination with
[ZFS delegation](https://www.freebsd.org/doc/handbook/zfs-zfs-allow.html).

Also, there is the possibility to run it in a jail on FreeBSD by delegating a dataset to the jail.

However, until we get around documenting those setups, you will have to run zrepl as root or experiment yourself :)

## Installation

zrepl is currently not packaged on any operating system. Signed & versioned releases are planned but not available yet.

Check out the sources yourself, compile and install to the zrepl user's `$PATH`.<br />
**Note**: if the zrepl binary is not in `$PATH`, you will have to adjust the examples in the [tutorial]({{< relref "tutorial/_index.md" >}}).

```bash
# NOTE: you may want to checkout & build as an unprivileged user
cd /root
git clone https://github.com/zrepl/zrepl.git
cd zrepl
go build -o zrepl
cp zrepl /usr/local/bin/zrepl
rehash
# see if it worked
zrepl help
```



## Configuration & Runtime Directories

### Main Configuration File

The main configuration file lives in either

* `/etc/zrepl/zrepl.yml`
* `/usr/local/etc/zrepl/zrepl.yml`

The file locations are check in that order. Alternative locations must be specified using a command line flag.

### Runtime Directories

**Source jobs** with `serve.type = stdinserver` require a private runtime directory. ([Why?]({{< relref "configuration/transports.md#stdinserver" >}}))

The default is `/var/run/zrepl/stdinserver` and it must only be accessible by the user
that runs `zrepl daemon` and `zrepl stdinserver`:

```bash
mkdir -p /var/run/zrepl/stdinserver
chmod -R 0700 /var/run/zrepl
```

## Running the Daemon

All work zrepl done is performed by a daemon process.

There are no *rc(8)* or *systemd.service(5)* service definitions yet.

The daemon does not fork and writes all log output to stderr.

```bash
zrepl --config /etc/zrepl/zrepl.yml daemon
```

FreeBSD ships with the *daemon(8)* utility which is also a good start for writing an *rc(8)* file:

```bash
daemon -o /var/log/zrepl.log \
       -p /var/run/zrepl/daemon.pid \
       zrepl --config /usr/local/etc/zrepl/zrepl.yml daemon
```

{{% notice info %}}
Make sure to read the first lines of log output after the daemon starts: if the daemon cannot create the [stdinserver]({{< relref "configuration/transports.md#stdinserver" >}}) sockets
in the runtime directory, it will complain but not terminate as other tasks such as taking periodic snapshots might still work.
{{% / notice %}}

### Restarting

The daemon handles SIGINT and SIGTERM for graceful shutdown.

Graceful shutdown means at worst that a job will not be rescheduled for the next interval.

The daemon exits as soon as all jobs have reported shut down.