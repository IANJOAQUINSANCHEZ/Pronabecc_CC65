package main

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"

	"Pronabecc_CC65/internal"
)

func main() {
	// FLAG PARA CONFIGURAR NÚMERO DE WORKERS DESDE LÍNEA DE COMANDOS
	numWorkers := flag.Int("workers", runtime.NumCPU(), "Número de workers en el pool")
	flag.Parse()

	fmt.Println("==========================================================")
	fmt.Println(" MOTOR DE RECOMENDACIONES PRONABEC — VERSIÓN CONCURRENTE")
	fmt.Printf(" Worker Pool: %d workers | GOMAXPROCS: %d\n", *numWorkers, runtime.GOMAXPROCS(0))
	fmt.Println("==========================================================")

	tiempoInicio := time.Now()

	// FASE 1: CARGA DEL CATÁLOGO DE BECAS
	fmt.Print("Cargando catálogo de becas...")
	tCarga := time.Now()
	becas, err := internal.CargarBecas("Becas_1M_Definitivo.csv")
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		return
	}
	fmt.Printf(" %d becas cargadas en %s\n", len(becas), time.Since(tCarga))

	// INDEXAR POR NIVEL — ESTRUCTURA READ-ONLY COMPARTIDA ENTRE WORKERS (SIN LOCKS)
	indicePorNivel := internal.IndexarBecasPorNivel(becas)
	fmt.Printf("Índice creado: %d niveles distintos\n", len(indicePorNivel))

	// FASE 2: CARGA DE ESTUDIANTES
	fmt.Print("Cargando perfiles de estudiantes...")
	tEst := time.Now()
	estudiantes, err := internal.CargarEstudiantes("Estudiantes_100k_Limpio.csv")
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		return
	}
	fmt.Printf(" %d estudiantes cargados en %s\n", len(estudiantes), time.Since(tEst))

	// FASE 3: WORKER POOL CON FAN-OUT / FAN-IN
	fmt.Printf("\nIniciando matching concurrente con %d workers...\n", *numWorkers)
	tMatch := time.Now()

	// CANAL DE TAREAS
	jobs := make(chan *internal.Estudiante, 1000)

	// CANAL DE RESULTADOS
	results := make(chan internal.ResultadoEstudiante, 1000)

	// WAITGROUPS PARA SINCRONIZACIÓN
	var wgWorkers sync.WaitGroup
	var wgWriter sync.WaitGroup

	// WRITER (FAN-IN)
	var totalEscritos int
	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		var err error
		totalEscritos, err = internal.EscribirResultadosStream("Recomendaciones_Concurrente.csv", results)
		if err != nil {
			fmt.Printf("Error en escritura: %v\n", err)
		}
	}()

	// WORKERS (FAN-OUT)
	for w := 0; w < *numWorkers; w++ {
		wgWorkers.Add(1)
		go func() {
			defer wgWorkers.Done()
			// CADA WORKER LEE DEL CANAL JOBS Y PROCESA
			for est := range jobs {
				recs := internal.RecomendarConIndice(est, indicePorNivel, becas)
				results <- internal.ResultadoEstudiante{
					IDPostulante:    est.IDPostulante,
					Recomendaciones: recs,
				}
			}
		}()
	}

	// PRODUCTOR
	for _, est := range estudiantes {
		jobs <- est
	}
	close(jobs)

	// ESPERAMOS A QUE TODOS LOS WORKERS TERMINEN
	wgWorkers.Wait()

	// CERRAR CANAL DE RESULTADOS PARA QUE EL ESCRITOR TERMINE
	close(results)

	// ESPERAMOS A QUE EL ESCRITOR TERMINE DE GUARDAR
	wgWriter.Wait()

	duracionMatch := time.Since(tMatch)
	fmt.Printf("Matching completado en %s\n", duracionMatch)

	// MÉTRICAS FINALES
	duracionTotal := time.Since(tiempoInicio)
	throughput := float64(len(estudiantes)) / duracionTotal.Seconds()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Println("\n==========================================================")
	fmt.Println(" RESUMEN CONCURRENTE")
	fmt.Println("==========================================================")
	fmt.Printf("  Workers:                %d\n", *numWorkers)
	fmt.Printf("  Estudiantes procesados: %d\n", totalEscritos)
	fmt.Printf("  Becas en catálogo:      %d\n", len(becas))
	fmt.Printf("  Tiempo de matching:     %s\n", duracionMatch)
	fmt.Printf("  Tiempo total:           %s\n", duracionTotal)
	fmt.Printf("  Throughput:             %.0f estudiantes/seg\n", throughput)
	fmt.Printf("  Pico de RAM (Alloc):    %.2f MB\n", float64(memStats.TotalAlloc)/1024/1024)
	fmt.Printf("  GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("  NumCPU:                 %d\n", runtime.NumCPU())
	fmt.Println("==========================================================")
}
