/*
** accesso a sendmsg/recvmsg
*/

/*
gcc -O2 libmoka.c -shared -o libmoka.so
- oppure -
gcc -O2 -c libmoka.c && gcc -shared -o libmoka.so libmoka.o
- oppure -
gcc libmoka.c -Wall -c -fPIC -o libmoka.o
gcc libmoka.o -shared -Wl,-soname,libmoka.so -o libmoka.so

*/

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

#define OK 0;
#define ERR -1;

/* for platforms that don't provide CMSG_*  macros */
#ifndef ALIGNBYTES
#define ALIGNBYTES (sizeof(int) - 1)
#endif

#ifndef ALIGN
#define ALIGN(p) (((unsigned int)(p) + ALIGNBYTES) & ~ ALIGNBYTES)
#endif

#ifndef CMSG_LEN
#define CMSG_LEN(len) (ALIGN(sizeof(struct cmsghdr)) + ALIGN(len))
#endif

#ifndef CMSG_SPACE
#define CMSG_SPACE(len) (ALIGN(sizeof(struct cmsghdr)) + ALIGN(len))
#endif


/*
char* get_errstr() {
    return strerror(errno);
}

int local_errno;
#define ERRSTR_BUFFER_SIZE 256
char local_errstr[ERRSTR_BUFFER_SIZE];

int get_errno() {
    return local_errno;
}


char *get_local_errstr() {
    return local_errstr;
}
*/

int local_errno;
#define ERRSTR_BUFFER_SIZE 256
char local_errstr[ERRSTR_BUFFER_SIZE];

int get_errno() {
    return local_errno;
}

char *get_errstr() {
    return local_errstr;
}

void set_error(int en, char *es) {

    local_errno = errno;
    strncpy(local_errstr, es, ERRSTR_BUFFER_SIZE - 1);
    local_errstr[ERRSTR_BUFFER_SIZE - 1] = '\0';
    
}




int send_fd(int sockfd, int fd) {

	char tmp[CMSG_SPACE(sizeof(int))];
	struct cmsghdr *cmsg;
	struct iovec iov;
	struct msghdr msg;
	char ch = '\0';

	memset(&msg, 0, sizeof(msg));
	msg.msg_control = (caddr_t) tmp;
	msg.msg_controllen = CMSG_LEN(sizeof(int));
	cmsg = CMSG_FIRSTHDR(&msg);
	cmsg->cmsg_len = CMSG_LEN(sizeof(int));
	cmsg->cmsg_level = SOL_SOCKET;
	cmsg->cmsg_type = SCM_RIGHTS;
	*(int *)CMSG_DATA(cmsg) = fd;
	iov.iov_base = &ch;
	iov.iov_len = 1;
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;

	if (sendmsg(sockfd, &msg, 0) != 1)
		return -1;

	return 0;

}


int recv_fd(int sockfd) {

	char tmp[CMSG_SPACE(sizeof(int))];
	struct cmsghdr *cmsg;
	struct iovec iov;
	struct msghdr msg;
	char ch = '\0';

	memset(&msg, 0, sizeof(msg));
	iov.iov_base = &ch;
	iov.iov_len = 1;
	msg.msg_iov = &iov;
	msg.msg_iovlen = 1;
	msg.msg_control = tmp;
	msg.msg_controllen = sizeof(tmp);

	if (recvmsg(sockfd, &msg, 0) <= 0)
		return -1;
	cmsg = CMSG_FIRSTHDR(&msg);
	return *(int *) CMSG_DATA(cmsg);

}


int fastcgi_parse_name_value_buffer(char *in_buffer, int in_size, char *out_buffer, int out_size) {

    int nsize, vsize;
    char *stop = in_buffer + in_size;
    char *out_limit = out_buffer + out_size;
    
    while (in_buffer < stop) {
        
        // name size
        if (*in_buffer & 0x80) {
            nsize = (*in_buffer++ & 0x7f) << 24;
            nsize += (*in_buffer++) << 16;
            nsize += (*in_buffer++) << 8;
            nsize += (*in_buffer++);
        } else {
            nsize = (int) (*in_buffer++);
        }
        
        // value size
        if (*in_buffer & 0x80) {
            vsize = (*in_buffer++ & 0x7f) << 24;
            vsize += (*in_buffer++) << 16;
            vsize += (*in_buffer++) << 8;
            vsize += (*in_buffer++);
        } else {
            vsize = (int) (*in_buffer++);
        }
        
        if ((out_buffer + nsize + 1) >= out_limit) {
            set_error(-1, "output buffer overflow (1)");
            return ERR;
        }
        
        memcpy(out_buffer, in_buffer, nsize);
        in_buffer += nsize;
        out_buffer += nsize;
        *out_buffer++ = '\n';

        if ((out_buffer + vsize + 1) >= out_limit) {
            set_error(-1, "output buffer overflow (2)");
            return ERR;
        }
        
        memcpy(out_buffer, in_buffer, vsize);
        in_buffer += vsize;
        out_buffer += vsize;
        *out_buffer++ = '\n';
                
    }
    
    // sostituisco l'ultimo \n con il fine stringa
    *(out_buffer - 1) = '\0';
    
    return OK;

}


int fastcgi_stream_into_buffer(int read_fd, int req_id, int type, char *out_buffer, int out_size) {

    char buffer[65536];
    int c, read_size = 0, padding_size, content_size;
    
    while (1) {
    
        c = read(read_fd, buffer, 8);       // intestazione del record fastcgi
        if (c == -1) {
            set_error(errno, strerror(errno));
            return ERR;
        } else if (c != 8) {
            set_error(-1, "short header");
            return ERR;
        }
    
        if (buffer[1] != type) {
            set_error(-1, "wrong type");
            return ERR;             
        }
        
        if (req_id != ((buffer[2] << 8) + buffer[3])) {
            set_error(-1, "wrong req_id");
            return ERR;             // id richiesta errato
        }

        content_size = ((u_char)buffer[4] << 8) + (u_char)buffer[5];
        padding_size = buffer[6];

        if (content_size == 0) {
            break;          // stream terminato
        }
        
        if (content_size > out_size) {
            set_error(-1, "output buffer overflow");
            return ERR;     
        }
        
        c = read(read_fd, out_buffer, content_size);
        if (c == -1) {
            set_error(errno, strerror(errno));
            return ERR;
        } else if (c != content_size) {
            set_error(-1, "not enough content data");
            return ERR;
        }

        if (padding_size != 0) {
            // consumo dei byte di padding
            c = read(read_fd, buffer, padding_size);
            if (c == -1) {
                set_error(errno, strerror(errno));
                return ERR;
            } else if (c != padding_size) {
                set_error(-1, "not enough padding data");
                return ERR;
            }
        }
                
        read_size += content_size;
        out_buffer += content_size;
        out_size -= content_size;
        
    }
    
    return read_size;
    
}


/* ******************************************************************* */

/*

from ctypes import cdll, c_int
dll = cdll.LoadLibrary('./libmoka.so')
dll.recv_fd

*/




