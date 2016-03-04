## Additional Step to use the normal mount and umount commands with cfsfuse

`ln -srf $GOPATH/bin/cfswrap /sbin/mount.cfs`


## Command line format and required options

    cfs mount device path_to_mount_point  -o [MOUNT OPTIONS]
      Command line arguments are positional
        command                 "mount" is required
        device                  region://<account number>/<file system name> is required
        path_to_mount_point     is required
        -o [List of Options]    the -o is required
            host=[ipaddress:port]     is the required location of the formic service


###Example to mount filesystem:

* `mount -t cfs unknown /mnt/cfsdrive -o host=localhost:8445,debug,rw`


### Examples to unmount filesystem:

* `umount /mnt/cfsdrive`

* `fusermount -u /mnt/cfsdrive`

* `fusermount -uz /mnt/cfsdrive`


### Example of /etc/fstab entry

    # <file system>   <mount point>      <type>  <options>                  <dump>  <pass>
    # ...
    unknown           /mnt/cfsdrive    cfs    rw,host=localhost:8445,debug    0       0
