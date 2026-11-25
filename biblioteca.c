#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <mqueue.h>
#include <string.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <fcntl.h>

pid_t pid_mostrador, pid_deposito;

#define NOMBRE_COLA "/cola_biblioteca"
#define TAM_MENSAJE 64

mqd_t cola;

int tiempo_aleatorio(int min, int max) {
    return rand() % (max - min + 1) + min;
}

void* busqueda(void* arg) {
    printf("[Hilo Búsqueda] Comienzo mi ejecución...\n");

    printf("[Hilo Búsqueda] Buscando ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 3));
    printf("[Hilo Búsqueda] Ejemplar %s localizado. Enviando a extracción...\n", (char *)arg);

    return NULL;
}

void* extraccion(void* arg) {
    printf("[Hilo Extracción] Comienzo mi ejecución...\n");

    printf("[Hilo Extracción] Extrayendo ejemplar %s de la estantería...\n", (char *)arg);
    sleep(tiempo_aleatorio(2, 4));
    printf("[Hilo Extracción] Ejemplar %s extraído. Enviando a registro...\n", (char *)arg);

    return NULL;
}

void* registro(void* arg) {
    printf("[Hilo Registro] Comienzo mi ejecución...\n");

    printf("[Hilo Registro] Registrando préstamo del ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 2));
    printf("[Hilo Registro] Préstamo del ejemplar %s registrado. Notificando al mostrador...\n", (char *)arg);

    return NULL;
}

int main(int argc, char* argv[]) {

    struct mq_attr atributos;

    atributos.mq_flags = 0;
    atributos.mq_maxmsg = 10;
    atributos.mq_msgsize = TAM_MENSAJE;
    atributos.mq_curmsgs = 0;

    mq_unlink(NOMBRE_COLA);

    cola = mq_open(NOMBRE_COLA, O_CREAT | O_RDWR, 0666, &atributos);

    if (cola == (mqd_t)-1) {
        perror("Error al crear la cola de mensajes");
        exit(EXIT_FAILURE);
    }

    pid_mostrador = fork();

    if (pid_mostrador != 0) {
        pid_deposito = fork();
        if (pid_deposito != 0) {
            /* Proceso padre */

            printf("[Proceso Padre] Esperando a que terminen mis hijos...\n");

            waitpid(pid_mostrador, NULL, 0);
            waitpid(pid_deposito, NULL, 0);

            mq_close(cola);
            mq_unlink(NOMBRE_COLA);

            printf("[Proceso Padre] Ejecución finalizada.\n");
        } else {
            /* Proceso Depósito */
            printf("[Proceso Depósito] Comienzo mi ejecución...\n");

            char mensaje[TAM_MENSAJE];

            pthread_t hilo_busqueda, hilo_extraccion, hilo_registro;

            while(1) {

                if (mq_receive(cola, mensaje, TAM_MENSAJE, NULL) != -1) {

                    printf("[Proceso Depósito] Recibida solicitud del ejemplar %s.\n", mensaje);

                    // Creación de los tres hilos
                    pthread_create(&hilo_busqueda, NULL, busqueda, mensaje);
                    pthread_create(&hilo_extraccion, NULL, extraccion, mensaje);
                    pthread_create(&hilo_registro, NULL, registro, mensaje);

                    // Esperamos a que finalicen los tres hilos
                    pthread_join(hilo_busqueda, NULL);
                    pthread_join(hilo_extraccion, NULL);
                    pthread_join(hilo_registro, NULL);

                    printf("[Proceso Depósito] Solicitud del ejemplar %s completada.\n", mensaje);

                } else {
                    perror("Error al recibir la solicitud");
                }
            }
        }
    } else {
        /* Proceso Mostrador */
        printf("[Proceso Mostrador] Comienzo mi ejecución...\n");
        int num_solicitud = 0;
        char mensaje[TAM_MENSAJE];

        while (1) {

            sleep(tiempo_aleatorio(3, 8));
            printf("[Proceso Mostrador] Recibida solicitud de préstamo nº %d. Enviando al depósito...\n", num_solicitud);

            snprintf(mensaje, TAM_MENSAJE, "%d", num_solicitud);

            if (mq_send(cola, mensaje, strlen(mensaje) + 1, 0) == -1) {
                perror("Error al enviar la solicitud");
            }

            num_solicitud++;

        }
    }

    exit(0);
}
