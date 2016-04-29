go build . && fusermount -u /tmp/test && ./cfs /tmp/test


```
Getting Started with CFS:
# install fuse
apt-get install fuse
# install cfs
wget https://github.com/getcfs/cfs-binary-release/releases/download/0.1.12/cfs
echo -e '#!/bin/sh\ncfs mount $1 $2 -o $4 > /dev/null &' > mount.cfs
chmod +x cfs mount.cfs
mv cfs mount.cfs /sbin/
# create the filesystem
cfs -T <token> create iad://<account_id> -name <fs_name>
# grant access to the filesystem
ifconfig
cfs -T <token> grant iad://<account_id>/<fs_id> -addr <ip> 
# mount the filesystem
mkdir -p /mnt/<fs_name>
echo “iad://<fs_id> /mnt/<fs_name> cfs rw 0 0” >> /etc/fstab
mount /mnt/<fs_name>
# optional mount methods
cfs mount iad://<fs_id> /mnt/<fs_name> -o debug
mount -t cfs iad://<fs_id> /mnt/<fs_name>
# unmount the filesystem
umount /mnt/<fs_name>
fusermount -u /mnt/<fs_name>  # use if umount fails
```
