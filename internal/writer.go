package internal

import (
	"encoding/csv"
	"fmt"
	"os"
)

// ESCRIBE LAS RECOMENDACIONES EN UN ARCHIVO CSV.
func EscribirResultados(ruta string, resultados []ResultadoEstudiante) error {
	file, err := os.Create(ruta)
	if err != nil {
		return fmt.Errorf("no se pudo crear %s: %w", ruta, err)
	}
	defer file.Close()

	// BOM PARA COMPATIBILIDAD CON EXCEL
	file.WriteString("\xEF\xBB\xBF")

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"ID_Postulante", "Rank", "Beca_Nombre", "Score",
		"Nivel", "Institucion", "Sede",
	})

	for _, res := range resultados {
		for _, rec := range res.Recomendaciones {
			writer.Write([]string{
				rec.IDPostulante,
				fmt.Sprintf("%d", rec.Rank),
				rec.BecaNombre,
				fmt.Sprintf("%d", rec.Score),
				rec.Nivel,
				rec.Institucion,
				rec.Sede,
			})
		}
	}

	return nil
}

// ESCRIBE RECOMENDACIONES AL CSV DESDE UN CANAL (FAN-IN)
func EscribirResultadosStream(ruta string, resultados <-chan ResultadoEstudiante) (int, error) {
	file, err := os.Create(ruta)
	if err != nil {
		return 0, fmt.Errorf("no se pudo crear %s: %w", ruta, err)
	}
	defer file.Close()

	file.WriteString("\xEF\xBB\xBF")
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{
		"ID_Postulante", "Rank", "Beca_Nombre", "Score",
		"Nivel", "Institucion", "Sede",
	})

	total := 0
	for res := range resultados {
		for _, rec := range res.Recomendaciones {
			writer.Write([]string{
				rec.IDPostulante,
				fmt.Sprintf("%d", rec.Rank),
				rec.BecaNombre,
				fmt.Sprintf("%d", rec.Score),
				rec.Nivel,
				rec.Institucion,
				rec.Sede,
			})
		}
		total++
	}

	return total, nil
}
