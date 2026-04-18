package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

type APIResponse struct {
	Total   int `json:"total"`
	Records int `json:"records"`
	Rows    []struct {
		Cell []interface{} `json:"cell"`
	} `json:"rows"`
}

func fetchPage(dataset string, page int, client *http.Client) (*APIResponse, error) {
	url := fmt.Sprintf("https://datosabiertos.pronabec.gob.pe/Dataset/Listar%s?_search=false&nd=%d&rows=100&page=%d&sidx=NRO_FILA&sord=asc", dataset, time.Now().UnixMilli(), page)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return nil, fmt.Errorf("SERVER_BUSY")
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	return &apiResp, nil
}

func procesarDataset(dataset string, client *http.Client, semGlobal chan struct{}, wgMain *sync.WaitGroup) {
	defer wgMain.Done() // AVISA A MAIN QUE EL DATASET TERMINÓ COMPLETAMENTE

	fmt.Printf("Iniciando Dataset: %s\n", dataset)

	semGlobal <- struct{}{}
	initialData, err := fetchPage(dataset, 1, client)
	<-semGlobal

	if err != nil || initialData == nil || initialData.Records == 0 {
		fmt.Printf("No se pudo iniciar %s\n", dataset)
		return
	}

	totalPages := initialData.Total
	var wg sync.WaitGroup
	var mu sync.Mutex
	var allRecords [][]interface{}

	for _, row := range initialData.Rows {
		allRecords = append(allRecords, row.Cell)
	}
	// MICRO-CONCURRENCIA
	// ACA SE APLICO A MICRO CONCURRENCIA DESCRITA EN EL DOCUMENTO, DONDE CADA DATASET TIENE SU PROPIO WAITGROUP Y MUTEX PARA CONTROLAR SU PROPIO ESTADO INTERNO, PERO TODOS COMPARTEN EL SEMÁFORO GLOBAL PARA NO SOBRECARGAR EL API DE PRONABEC
	for page := 2; page <= totalPages; page++ {
		wg.Add(1)

		go func(p int) {
			defer wg.Done()

			// SEMÁFORO GLOBAL
			semGlobal <- struct{}{}
			defer func() { <-semGlobal }()

			var pageData *APIResponse
			var fetchErr error

			for reintentos := 0; reintentos < 5; reintentos++ {
				pageData, fetchErr = fetchPage(dataset, p, client)
				if fetchErr == nil {
					break
				}
				time.Sleep(2 * time.Second)
			}

			if pageData != nil {
				// AISLAMIENTO DE ESTADO PORQUE CADA DATASET TIENE SU PROPIO WAITGROUP Y MUTEX, ASI EVITAMOS DEADLOCKS
				mu.Lock()
				for _, row := range pageData.Rows {
					allRecords = append(allRecords, row.Cell)
				}
				mu.Unlock()
				fmt.Printf("%s -> Pág %d/%d lista\n", dataset, p, totalPages)
			} else {
				fmt.Printf("%s -> Falló la página %d permanentemente\n", dataset, p)
			}
		}(page)
	}

	wg.Wait() // ESPERA QUE TODAS LAS PAGINAS DE ESTE DATASET TERMINEN, PERO NO BLOQUEA A LOS OTROS DATASETS PORQUE CADA UNO TIENE SU PROPIO WAITGROUP, ASÍ ASEGURAMOS QUE NO SE PIERDE INFORMACIÓN

	filename := fmt.Sprintf("%s.csv", dataset)
	file, _ := os.Create(filename)
	defer file.Close()
	file.WriteString("\xEF\xBB\xBF")

	for _, row := range allRecords {
		for i, col := range row {
			val := ""
			if col != nil {
				val = fmt.Sprintf("%v", col)
			}
			file.WriteString(val)
			if i < len(row)-1 {
				file.WriteString(",")
			}
		}
		file.WriteString("\n")
	}

	fmt.Printf(" ÉXITO FINAL: %s guardado (%d filas)\n", dataset, len(allRecords))
}

func main() {
	misDatasets := []string{
		"Credito18Postulacion",
		"PerdidaDeBecas",
		"NotaPromedioDelPostulantePorRegion",
		"EstadosDelBecario",
		"Credito18EvaluacionSolicitud",
		"Credito18DatosSolicitante",
		"Convocatorias",
	}

	client := &http.Client{Timeout: 60 * time.Second}
	tiempoInicio := time.Now()

	var wgMain sync.WaitGroup

	// SEMÁFORO GLOBAL PARA CONTROLAR LA CANTIDAD TOTAL DE PETICIONES CONCURRENTES HACIA EL API DE PRONABEC, INDEPENDIENTEMENTE DE CUÁNTOS DATASETS SE ESTÉN PROCESANDO AL MISMO TIEMPO
	semGlobal := make(chan struct{}, 6)

	fmt.Printf("\n⚡ INICIANDO EXTRACCIÓN MACRO-CONCURRENTE ⚡\n\n")

	// MACRO-CONCURRENCIA EXPLICADA EN EL DOCUMENTO, DONDE CADA DATASET SE PROCESA EN UNA GOROUTINE SEPARADA, PERO TODAS COMPARTEN EL SEMÁFORO GLOBAL PARA CONTROLAR LA CARGA SOBRE EL API DE PRONABEC
	for _, dataset := range misDatasets {
		wgMain.Add(1)
		go procesarDataset(dataset, client, semGlobal, &wgMain)
	}

	// EL MAIN ESPERARÁ QUE TODOS LOS DATASETS TERMINEN PARA QUE NO SE PIERDA INFORMACIÓN SI ALGUNO TERMINA ANTES QUE OTRO
	wgMain.Wait()

	fmt.Printf("\nEXTRACCIÓN MASIVA FINALIZADA\n")
	fmt.Printf("Tiempo total de ejecución: %s\n", time.Since(tiempoInicio))
}
