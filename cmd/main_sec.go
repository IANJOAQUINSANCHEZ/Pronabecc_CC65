package main

import (
	"fmt"
	"runtime"
	"time"

	"Pronabecc_CC65/internal"
)

func main() {
	runtime.GOMAXPROCS(1) // SE FUERZA UN SOLO HILO

	fmt.Println("==========================================================")
	fmt.Println(" MOTOR DE RECOMENDACIONES PRONABEC — VERSIÓN SECUENCIAL")
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

	// INDEXAR POR NIVEL+GESTIÓN PARA PODA COMPUESTA O(1)
	indicePorNivel := internal.IndexarBecasCompuesto(becas)
	fmt.Printf("Índice creado: %d niveles distintos\n", len(indicePorNivel))
	for nivel, grupo := range indicePorNivel {
		fmt.Printf("  [%s]: %d becas\n", nivel, len(grupo))
	}

	// FASE 2: CARGA DE ESTUDIANTES (STREAMING)
	fmt.Println("La carga de estudiantes se realizará por flujo (streaming).")

	// FASE 3: MATCHING SECUENCIAL — UN ESTUDIANTE A LA VEZ
	fmt.Println("\nIniciando matching secuencial...")
	tMatch := time.Now()

	estudiantesChan := make(chan *internal.Estudiante, 1000)
	go func() {
		err := internal.StreamEstudiantes("Estudiantes_Final.csv", estudiantesChan)
		if err != nil {
			fmt.Printf("Error en streaming de estudiantes: %v\n", err)
		}
		close(estudiantesChan)
	}()

	resultados := make([]internal.ResultadoEstudiante, 0, 100000)
	procesados := 0

	for est := range estudiantesChan {
		recs := internal.RecomendarConIndice(est, indicePorNivel, becas)
		resultados = append(resultados, internal.ResultadoEstudiante{
			IDPostulante:    est.IDPostulante,
			Recomendaciones: recs,
		})
		procesados++

		if procesados%10000 == 0 {
			elapsed := time.Since(tMatch)
			rate := float64(procesados) / elapsed.Seconds()
			fmt.Printf("  Procesados: %d — %.0f est/s\n", procesados, rate)
		}
	}

	duracionMatch := time.Since(tMatch)
	fmt.Printf("Matching completado en %s\n", duracionMatch)

	// FASE 4: ESCRITURA DE RESULTADOS
	fmt.Print("Escribiendo resultados...")
	tEscritura := time.Now()
	err = internal.EscribirResultados("Recomendaciones_Secuencial.csv", resultados)
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		return
	}
	fmt.Printf(" guardado en %s\n", time.Since(tEscritura))

	// MÉTRICAS FINALES
	duracionTotal := time.Since(tiempoInicio)
	throughput := float64(procesados) / duracionTotal.Seconds()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Println("\n==========================================================")
	fmt.Println(" RESUMEN SECUENCIAL")
	fmt.Println("==========================================================")
	fmt.Printf("  Estudiantes procesados: %d\n", procesados)
	fmt.Printf("  Becas en catálogo:      %d\n", len(becas))
	fmt.Printf("  Tiempo de matching:     %s\n", duracionMatch)
	fmt.Printf("  Tiempo total:           %s\n", duracionTotal)
	fmt.Printf("  Throughput:             %.0f estudiantes/seg\n", throughput)
	fmt.Printf("  Pico de RAM (Alloc):    %.2f MB\n", float64(memStats.TotalAlloc)/1024/1024)
	fmt.Printf("  GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Println("==========================================================")
}
