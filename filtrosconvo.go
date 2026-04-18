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
	url := fmt.Sprintf("https://datosabiertos.pronabec.gob.pe/Dataset/Listar%s?_search=false&nd=%d&rows=500&page=%d&sidx=NRO_FILA&sord=asc", dataset, time.Now().UnixMilli(), page)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Accept", "application/json, text/javascript, */*; q=0.01")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP_ERROR_%d", resp.StatusCode)
	}

	var apiResp APIResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, err
	}

	return &apiResp, nil
}

func procesarDatasetSeguro(dataset string, client *http.Client) {
	fmt.Printf("\n======================================================\n")
	fmt.Printf(" Iniciando Dataset: %s\n", dataset)

	var initialData *APIResponse
	var err error
	for r := 0; r < 3; r++ {
		initialData, err = fetchPage(dataset, 1, client)
		if err == nil && initialData != nil && initialData.Records > 0 {
			break
		}
		time.Sleep(3 * time.Second)
	}

	if initialData == nil || initialData.Records == 0 {
		fmt.Printf(" %s falló en el inicio.\n", dataset)
		return
	}

	totalPages := initialData.Total

	// LÍMITE DE 200 PÁGINAS PORQUE SI NO SE CAE EL API DE PRONABEC

	if totalPages > 200 {
		fmt.Printf("El dataset tiene %d páginas, pero lo limitaremos a 200 porque se cae.\n", totalPages)
		totalPages = 200
	}

	fmt.Printf("Total a procesar: %d páginas (Aprox. %d filas)\n", totalPages, totalPages*500)
	fmt.Printf("======================================================\n")

	var wg sync.WaitGroup
	var mu sync.Mutex
	var allRecords [][]interface{}

	for _, row := range initialData.Rows {
		allRecords = append(allRecords, row.Cell)
	}

	sem := make(chan struct{}, 8)

	for page := 2; page <= totalPages; page++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(p int) {
			defer wg.Done()
			defer func() { <-sem }()

			var pageData *APIResponse
			var fetchErr error

			for reintentos := 0; reintentos < 5; reintentos++ {
				pageData, fetchErr = fetchPage(dataset, p, client)
				if fetchErr == nil {
					break
				}
				fmt.Printf("   [⏳ Reintento %d] %s Pág %d falló. Esperando 5s...\n", reintentos+1, dataset, p)
				time.Sleep(5 * time.Second)
			}

			if pageData != nil {
				mu.Lock()
				for _, row := range pageData.Rows {
					allRecords = append(allRecords, row.Cell)
				}
				mu.Unlock()
				fmt.Printf(" %s -> Pág %d/%d lista\n", dataset, p, totalPages)
			} else {
				fmt.Printf(" ERROR CRÍTICO: %s -> Se perdió la Pág %d\n", dataset, p)
			}
		}(page)
	}

	wg.Wait()

	filename := fmt.Sprintf("%s_Top200.csv", dataset)
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
	fmt.Printf("ÉXITO: %s guardado completo (%d filas)\n", dataset, len(allRecords))
}

func main() {
	misDatasets := []string{
		"NotasDeBecarios",
		"ConvocatoriaPorCarreraSede",
	}

	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 20
	t.MaxConnsPerHost = 10

	client := &http.Client{
		Timeout:   120 * time.Second,
		Transport: t,
	}

	tiempoInicio := time.Now()

	for _, dataset := range misDatasets {
		procesarDatasetSeguro(dataset, client)
		time.Sleep(5 * time.Second)
	}

	fmt.Printf("\n EXTRACCIÓN LIMITADA (200 PÁGS) FINALIZADA \n")
	fmt.Printf("Tiempo total de ejecución: %s\n", time.Since(tiempoInicio))
}
