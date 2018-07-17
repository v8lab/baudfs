## Optimize FUSE linux kernel module

In order to achieve maximum throughput performance, several FUSE kernel parameters have to be modified, such as FUSE_MAX_PAGES_PER_REQ and FUSE_DEFAULT_MAX_BACKGROUND.

```C
#define FUSE_MAX_PAGES_PER_REQ 256
#define FUSE_DEFAULT_MAX_BACKGROUND 32
```

## How to get Linux kernel source for CentOS

Download the corresponding src rpm, and use the following commands to install source code.

```bash
rpm -i kernel-3.10.0-327.28.3.el7.src.rpm 2>&1 | grep -v exist
cd ~/rpmbuild/SPECS
rpmbuild -bp --target=$(uname -m) kernel.spec
```

The source code is installed in ~/rpmbuild/BUILD/

## How to build against the current running Linux kernel

```bash
yum install kernel-devel-3.10.0-327.28.3.el7.x86_64

cd ~/rpmbuild/BUILD/kernel-3.10.0-327.28.3.el7/linux-3.10.0-327.28.3.el7.x86_64/fs/fuse
make -C /lib/modules/`uname -r`/build M=$PWD
```

## How to insert kernel module

```bash
cp fuse.ko /lib/modules/`uname -r`/kernel/fs/fuse

depmod -a
modprobe fuse
```
