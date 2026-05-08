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

	// FASE 2: CARGA DE ESTUDIANTES
	fmt.Print("Cargando perfiles de estudiantes...")
	tEst := time.Now()
	estudiantes, err := internal.CargarEstudiantes("Estudiantes_Final.csv")
	if err != nil {
		fmt.Printf("\nError: %v\n", err)
		return
	}
	fmt.Printf(" %d estudiantes cargados en %s\n", len(estudiantes), time.Since(tEst))

	// FASE 3: MATCHING SECUENCIAL — UN ESTUDIANTE A LA VEZ
	fmt.Println("\nIniciando matching secuencial...")
	tMatch := time.Now()

	resultados := make([]internal.ResultadoEstudiante, 0, len(estudiantes))
	procesados := 0

	for _, est := range estudiantes {
		recs := internal.RecomendarConIndice(est, indicePorNivel, becas)
		resultados = append(resultados, internal.ResultadoEstudiante{
			IDPostulante:    est.IDPostulante,
			Recomendaciones: recs,
		})
		procesados++

		if procesados%10000 == 0 {
			elapsed := time.Since(tMatch)
			rate := float64(procesados) / elapsed.Seconds()
			eta := time.Duration(float64(len(estudiantes)-procesados)/rate) * time.Second
			fmt.Printf("  Procesados: %d/%d (%.1f%%) — %.0f est/s — ETA: %s\n",
				procesados, len(estudiantes),
				float64(procesados)/float64(len(estudiantes))*100,
				rate, eta)
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
	throughput := float64(len(estudiantes)) / duracionTotal.Seconds()

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Println("\n==========================================================")
	fmt.Println(" RESUMEN SECUENCIAL")
	fmt.Println("==========================================================")
	fmt.Printf("  Estudiantes procesados: %d\n", len(estudiantes))
	fmt.Printf("  Becas en catálogo:      %d\n", len(becas))
	fmt.Printf("  Tiempo de matching:     %s\n", duracionMatch)
	fmt.Printf("  Tiempo total:           %s\n", duracionTotal)
	fmt.Printf("  Throughput:             %.0f estudiantes/seg\n", throughput)
	fmt.Printf("  Pico de RAM (Alloc):    %.2f MB\n", float64(memStats.TotalAlloc)/1024/1024)
	fmt.Printf("  GOMAXPROCS:             %d\n", runtime.GOMAXPROCS(0))
	fmt.Println("==========================================================")
}
