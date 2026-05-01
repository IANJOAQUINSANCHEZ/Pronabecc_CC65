#define NUM_ESTUDIANTES  6
#define NUM_WORKERS      3
#define BUFFER_JOBS      4
#define BUFFER_RESULTS   4
#define TOP_N            2

mtype = { ESTUDIANTE, RECOMENDACION, DONE };

chan jobs    = [BUFFER_JOBS]    of { mtype, int };
chan results = [BUFFER_RESULTS] of { mtype, int };

byte workers_done         = 0;
byte estudiantes_procesados = 0;
bool writer_termino       = false;


/* ════════════════════════════════════════════════════════════════════════════
 * PROCESO: Productor
 * ════════════════════════════════════════════════════════════════════════════ */
active proctype Productor() {
    int i = 0;

    do
    :: i < NUM_ESTUDIANTES ->
        jobs ! ESTUDIANTE, i;
        i++
    :: i >= NUM_ESTUDIANTES ->
        break
    od;

    i = 0;
    do
    :: i < NUM_WORKERS ->
        jobs ! DONE, -1;
        i++
    :: i >= NUM_WORKERS ->
        break
    od;

    printf("[Productor] Todos los estudiantes enviados. Canal jobs cerrado.\n")
}


/* ════════════════════════════════════════════════════════════════════════════
 * PROCESO: Worker
 * ════════════════════════════════════════════════════════════════════════════ */
proctype Worker(byte id) {
    mtype msg_type;
    int   est_id;
    int   rec;

    do
    :: jobs ? msg_type, est_id ->
        if
        :: msg_type == ESTUDIANTE ->
            rec = 0;
            do
            :: rec < TOP_N ->
                results ! RECOMENDACION, est_id;
                rec++
            :: rec >= TOP_N ->
                break
            od;

            atomic { estudiantes_procesados++ };
            printf("[Worker %d] Estudiante %d procesado.\n", id, est_id)

        :: msg_type == DONE ->
            break
        fi
    od;

    atomic { workers_done++ };
    printf("[Worker %d] Terminado. workers_done = %d\n", id, workers_done)
}


/* ════════════════════════════════════════════════════════════════════════════
 * PROCESO: Coordinador
 * ════════════════════════════════════════════════════════════════════════════ */
active proctype Coordinador() {
    atomic {
        workers_done == NUM_WORKERS ->
        printf("[Coordinador] Todos los Workers terminaron. Cerrando canal results.\n")
    };

    results ! DONE, -1;

    atomic {
        writer_termino == true ->
        printf("[Coordinador] Writer terminado. Sistema completo.\n")
    }
}


/* ════════════════════════════════════════════════════════════════════════════
 * PROCESO: Writer (Fan-In)
 * ════════════════════════════════════════════════════════════════════════════ */
active proctype Writer() {
    mtype msg_type;
    int   est_id;
    int   total_escritas = 0;

    do
    :: results ? msg_type, est_id ->
        if
        :: msg_type == RECOMENDACION ->
            total_escritas++;
            printf("[Writer] Recomendacion guardada. Total: %d\n", total_escritas)

        :: msg_type == DONE ->
            break
        fi
    od;

    writer_termino = true;
    printf("[Writer] Finalizado. Total recomendaciones escritas: %d\n", total_escritas)
}


/* ════════════════════════════════════════════════════════════════════════════
 * INSTANCIACION DEL WORKER POOL
 * ════════════════════════════════════════════════════════════════════════════ */
init {
    atomic {
        run Worker(0);
        run Worker(1);
        run Worker(2)
    };
    printf("[Init] Worker Pool de %d workers iniciado.\n", NUM_WORKERS)
}


/* ════════════════════════════════════════════════════════════════════════════
 * PROPIEDADES LTL (Linear Temporal Logic)
 * ════════════════════════════════════════════════════════════════════════════ */
ltl completitud {
    <> (estudiantes_procesados == NUM_ESTUDIANTES)
}

ltl orden_terminacion {
    [] (writer_termino -> (workers_done == NUM_WORKERS))
}
