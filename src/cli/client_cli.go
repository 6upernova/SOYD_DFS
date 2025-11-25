package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/6upernova/SOYD_DFS/src/client"
	"github.com/6upernova/SOYD_DFS/src/transport"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Estilos retro terminal - Lost Media aesthetic
var (
	// Colores retro CRT
	crtGreen     = lipgloss.Color("#33FF33")
	crtAmber     = lipgloss.Color("#FFAA00")
	crtDimGreen  = lipgloss.Color("#00AA00")
	crtBg        = lipgloss.Color("#000000")
	crtRed       = lipgloss.Color("#FF3333")
	crtCyan      = lipgloss.Color("#00FFFF")

	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(crtCyan).
			Background(crtBg).
			Padding(0, 1).
			MarginBottom(1)

	selectedStyle = lipgloss.NewStyle().
			Foreground(crtBg).
			Background(crtGreen).
			Bold(true)

	normalStyle = lipgloss.NewStyle().
			Foreground(crtGreen)

	dimStyle = lipgloss.NewStyle().
			Foreground(crtDimGreen)

	helpStyle = lipgloss.NewStyle().
			Foreground(crtDimGreen).
			Italic(true)

	errorStyle = lipgloss.NewStyle().
			Foreground(crtRed).
			Bold(true)

	successStyle = lipgloss.NewStyle().
			Foreground(crtGreen).
			Bold(true)

	warningStyle = lipgloss.NewStyle().
			Foreground(crtAmber).
			Bold(true)

	infoStyle = lipgloss.NewStyle().
			Foreground(crtAmber)

	panelBorderStyle = lipgloss.NewStyle().
				Border(lipgloss.NormalBorder()).
				BorderForeground(crtDimGreen).
				Padding(0, 1)
)

type screen int

const (
	menuScreen screen = iota
	inputScreen
	listScreen
	infoScreen
	loggerScreen
	catScreen
)

type logEntry struct {
	timestamp time.Time
	message   string
	logType   string // "info", "error", "success"
}

type model struct {
	currentScreen screen
	selectedCmd   string
	input         textinput.Model
	message       string
	messageType   string
	localFiles    []string
	remoteFiles   []string
	fileInfo      []transport.Label
	cursor        int
	listOffset    int
	dfsClient     *client.DFSClient
	width         int
	height        int
	logs          []logEntry
	logOffset     int
	catContent    string  
	catOffset     int
}

func initialModel(dfsClient *client.DFSClient) model {
	ti := textinput.New()
	ti.Placeholder = "filename.ext"
	ti.Focus()
	ti.CharLimit = 156
	ti.Width = 50
	ti.PromptStyle = lipgloss.NewStyle().Foreground(crtGreen)
	ti.TextStyle = lipgloss.NewStyle().Foreground(crtCyan)
	ti.PlaceholderStyle = lipgloss.NewStyle().Foreground(crtDimGreen)

	return model{
		currentScreen: menuScreen,
		input:         ti,
		localFiles:    dfsClient.GetLocalFiles(),
		remoteFiles:   []string{},
		dfsClient:     dfsClient,
		cursor:        0,
		listOffset:    0,
		width:         80,
		height:        24,
		logs:          []logEntry{},
		logOffset:     0,
	}
}

func (m *model) addLog(message string, logType string) {
	m.logs = append(m.logs, logEntry{
		timestamp: time.Now(),
		message:   message,
		logType:   logType,
	})
	// Mantener solo los últimos 100 logs
	if len(m.logs) > 100 {
		m.logs = m.logs[1:]
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil
	case tea.KeyMsg:
		switch m.currentScreen {
		case menuScreen:
			return m.updateMenu(msg)
		case inputScreen:
			return m.updateInput(msg)
		case listScreen:
			return m.updateList(msg)
		case infoScreen:
			return m.updateInfo(msg)
		case loggerScreen:
			return m.updateLogger(msg)
		case catScreen:  // NUEVO
			return m.updateCat(msg)
		}
	}
	return m, nil
}

func (m model) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
		}
	case "down", "j":
		if m.cursor < 7 {
			m.cursor++
		}
	case "enter":
		commands := []string{"ls", "info", "get", "put", "remove", "cat", "logger", "quit"}
		m.selectedCmd = commands[m.cursor]

		switch m.selectedCmd {
		case "quit":
			return m, tea.Quit
		case "ls":
			m.addLog("Listing remote files...", "info")
			files, err := m.dfsClient.Ls()
			for _,file := range files{
				m.addLog("Files: %s",file)
			}
			if err != nil {
				m.message = fmt.Sprintf("ERROR: %v", err)
				m.messageType = "error"
				m.addLog(fmt.Sprintf("LS failed: %v", err), "error")
			} else {
				m.remoteFiles = files
				if len(files) == 0 {
					m.message = "WARNING: No files found on remote server"
					m.messageType = "warning"
					m.addLog("No remote files found", "warning")
				} else {
					m.message = fmt.Sprintf("OK: %d remote files found", len(files))
					m.messageType = "success"
					m.addLog(fmt.Sprintf("Found %d remote files", len(files)), "success")
				}
			}
			return m, nil
		case "info", "get","cat":
			m.currentScreen = inputScreen
			m.input.Placeholder = "filename.ext"
			m.input.SetValue("")
			m.message = ""
			return m, nil
		case "remove":
			m.currentScreen = inputScreen
			m.input.Placeholder = "filename.ext"
			m.input.SetValue("")
			m.message = ""
			return m, nil
		case "put":
			m.localFiles = m.dfsClient.GetLocalFiles()
			if len(m.localFiles) == 0 {
				m.message = "ERROR: No local files in ./local_files/"
				m.messageType = "error"
				m.addLog("No local files found", "error")
				return m, nil
			}
			m.currentScreen = listScreen
			m.cursor = 0
			m.listOffset = 0
			m.message = ""
			return m, nil
		case "logger":
			m.currentScreen = loggerScreen
			m.cursor = 0
			m.logOffset = len(m.logs) - 15
			if m.logOffset < 0 {
				m.logOffset = 0
			}
			return m, nil
		}
	}
	return m, nil
}

func (m model) updateInput(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg.String() {
	case "ctrl+c", "esc":
		m.currentScreen = menuScreen
		m.message = ""
		m.cursor = 0
		return m, nil
	case "enter":
		filename := strings.TrimSpace(m.input.Value())
		if filename == "" {
			m.message = "ERROR: Filename cannot be empty"
			m.messageType = "error"
			return m, nil
		}

		switch m.selectedCmd {
		case "info":
			m.addLog(fmt.Sprintf("Getting info for: %s", filename), "info")
			info, err := m.dfsClient.Info(filename)
			if err != nil {
				m.message = fmt.Sprintf("ERROR: %v", err)
				m.messageType = "error"
				m.addLog(fmt.Sprintf("INFO failed: %v", err), "error")
				m.currentScreen = menuScreen
				m.cursor = 0
			} else {
				m.fileInfo = info
				m.addLog(fmt.Sprintf("Got info for %s: %d blocks", filename, len(info)), "success")
				m.currentScreen = infoScreen
				m.cursor = 0
				m.listOffset = 0
				return m, nil
			}
		case "get":
			m.addLog(fmt.Sprintf("Downloading: %s", filename), "info")
			err := m.dfsClient.Get(filename)
			if err != nil {
				m.message = fmt.Sprintf("ERROR: %v", err)
				m.messageType = "error"
				m.addLog(fmt.Sprintf("GET failed: %v", err), "error")
			} else {
				m.message = fmt.Sprintf("OK: File '%s' downloaded", filename)
				m.messageType = "success"
				m.addLog(fmt.Sprintf("Downloaded: %s", filename), "success")
				m.localFiles = m.dfsClient.GetLocalFiles()
			}
			m.currentScreen = menuScreen
			m.cursor = 0
		case "remove":
			m.addLog(fmt.Sprintf("Removing: %s", filename), "info")
			err := m.dfsClient.Rm(filename)
			if err != nil {
				m.message = fmt.Sprintf("ERROR: %v", err)
				m.messageType = "error"
				m.addLog(fmt.Sprintf("Remove failed: %v", err), "error")
			} else {
				m.message = fmt.Sprintf("OK: File '%s' Removed", filename)
				m.messageType = "success"
				m.addLog(fmt.Sprintf("Removed: %s", filename), "success")
				m.remoteFiles, _ = m.dfsClient.Ls()
			}
			m.currentScreen = menuScreen
			m.cursor = 0
			case "cat":  // NUEVO CASO
				m.addLog(fmt.Sprintf("Reading: %s", filename), "info")
				content, err := m.dfsClient.Cat(filename)
				if err != nil {
					m.message = fmt.Sprintf("ERROR: %v", err)
					m.messageType = "error"
					m.addLog(fmt.Sprintf("CAT failed: %v", err), "error")
					m.currentScreen = menuScreen
					m.cursor = 0
				} else {
					m.catContent = content
					m.catOffset = 0
					m.addLog(fmt.Sprintf("Read %s: %d bytes", filename, len(content)), "success")
					m.currentScreen = catScreen
					return m, nil
			}
		}
		return m, nil

	}

	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m model) updateCat(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	lines := strings.Split(m.catContent, "\n")
	maxVisible := 15

	switch msg.String() {
	case "ctrl+c", "esc", "q", "enter":
		m.currentScreen = menuScreen
		m.message = ""
		m.cursor = 0
		m.catOffset = 0
		return m, nil
	case "up", "k":
		if m.catOffset > 0 {
			m.catOffset--
		}
	case "down", "j":
		if m.catOffset < len(lines)-maxVisible {
			m.catOffset++
		}
	case "pgup":
		m.catOffset -= maxVisible
		if m.catOffset < 0 {
			m.catOffset = 0
		}
	case "pgdown":
		m.catOffset += maxVisible
		if m.catOffset > len(lines)-maxVisible {
			m.catOffset = len(lines) - maxVisible
		}
		if m.catOffset < 0 {
			m.catOffset = 0
		}
	}
	return m, nil
}

func (m model) updateList(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	maxVisible := 10

	switch msg.String() {
	case "ctrl+c", "esc":
		m.currentScreen = menuScreen
		m.message = ""
		m.cursor = 0
		m.listOffset = 0
		return m, nil
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
			if m.cursor < m.listOffset {
				m.listOffset--
			}
		}
	case "down", "j":
		if m.cursor < len(m.localFiles)-1 {
			m.cursor++
			if m.cursor >= m.listOffset+maxVisible {
				m.listOffset++
			}
		}
	case "enter":
		if len(m.localFiles) > 0 && m.cursor < len(m.localFiles) {
			selectedFile := m.localFiles[m.cursor]
			filePath := "./local_files/" + selectedFile

			m.addLog(fmt.Sprintf("Uploading: %s", selectedFile), "info")
			err := m.dfsClient.Put(filePath)
			if err != nil {
				m.message = fmt.Sprintf("ERROR: %v", err)
				m.messageType = "error"
				m.addLog(fmt.Sprintf("PUT failed: %v", err), "error")
			} else {
				m.message = fmt.Sprintf("OK: File '%s' uploaded", selectedFile)
				m.messageType = "success"
				m.addLog(fmt.Sprintf("Uploaded: %s", selectedFile), "success")
				files, _ := m.dfsClient.Ls()
				m.remoteFiles = files
			}
			m.currentScreen = menuScreen
			m.cursor = 0
			m.listOffset = 0
		}
		return m, nil
	}
	return m, nil
}

func (m model) updateInfo(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	maxVisible := 8

	switch msg.String() {
	case "ctrl+c", "esc", "q", "enter":
		m.currentScreen = menuScreen
		m.message = ""
		m.cursor = 0
		m.listOffset = 0
		return m, nil
	case "up", "k":
		if m.cursor > 0 {
			m.cursor--
			if m.cursor < m.listOffset {
				m.listOffset--
			}
		}
	case "down", "j":
		if m.cursor < len(m.fileInfo)-1 {
			m.cursor++
			if m.cursor >= m.listOffset+maxVisible {
				m.listOffset++
			}
		}
	}
	return m, nil
}

func (m model) updateLogger(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	maxVisible := 15

	switch msg.String() {
	case "ctrl+c", "esc", "q", "enter":
		m.currentScreen = menuScreen
		m.message = ""
		m.cursor = 0
		return m, nil
	case "up", "k":
		if m.logOffset > 0 {
			m.logOffset--
		}
	case "down", "j":
		if m.logOffset < len(m.logs)-maxVisible {
			m.logOffset++
		}
	}
	return m, nil
}

func (m model) View() string {
	var s strings.Builder

	// Header con estilo retro
	header := titleStyle.Render("╔═══════════════════════════════════════════════════════════════════════════════╗")
	s.WriteString(header+"\n")
	s.WriteString(titleStyle.Render("║              DISTRIBUTED FILE SYSTEM - CLIENT TERMINAL v1.0                  ║")+"\n" )
	s.WriteString(titleStyle.Render("╚═══════════════════════════════════════════════════════════════════════════════╝") + "\n")

	switch m.currentScreen {
	case menuScreen:
		s.WriteString(m.renderMenu())
	case inputScreen:
		s.WriteString(m.renderInput())
	case listScreen:
		s.WriteString(m.renderList())
	case infoScreen:
		s.WriteString(m.renderInfo())
	case loggerScreen:
		s.WriteString(m.renderLogger())
	case catScreen:  // NUEVO
		s.WriteString(m.renderCat())
	}

	// Status bar
	if m.message != "" {
		s.WriteString("\n")
		statusBar := strings.Repeat("─", 79)
		s.WriteString(dimStyle.Render(statusBar) + "\n")

		switch m.messageType {
		case "error":
			s.WriteString(errorStyle.Render("[ ERR ] " + m.message))
		case "success":
			s.WriteString(successStyle.Render("[ OK  ] " + m.message))
		case "warning":
			s.WriteString(warningStyle.Render("[ WRN ] " + m.message))
		default:
			s.WriteString(normalStyle.Render("[ INFO] " + m.message))
		}
	}

	return s.String()
}

func (m model) renderMenu() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(normalStyle.Render("> MAIN MENU"))
	s.WriteString("\n\n")

	options := []string{
		"[1] LIST    - List remote files",
		"[2] INFO    - Get file information",
		"[3] GET     - Download file from DFS",
		"[4] PUT     - Upload file to DFS",
		"[5] REMOVE  - Remove file from DFS",
		"[6] CAT     - Display file content",  // NUEVA OPCIÓN
		"[7] LOGGER  - View operation logs",
		"[8] QUIT    - Exit program",
	}


	for i, opt := range options {
		if m.cursor == i {
			s.WriteString(selectedStyle.Render("> " + opt) + "\n")
		} else {
			s.WriteString(normalStyle.Render("  " + opt) + "\n")
		}
	}

	s.WriteString("\n")
	s.WriteString(helpStyle.Render("  [↑/↓] Navigate  [ENTER] Select  [Q] Quit"))
	s.WriteString("\n\n")

	// Panels lado a lado
	s.WriteString(m.renderFilePanels())

	return s.String()
}

func (m model) renderFilePanels() string {
	// Configuración de dimensiones
	panelWidth := 40
	panelHeight := 7
	
	// Panel Izquierdo - Archivos Locales
	var localLines []string
	localLines = append(localLines, successStyle.Render("LOCAL FILES"))
	localLines = append(localLines, dimStyle.Render(strings.Repeat("─", panelWidth-4)))
	
	if len(m.localFiles) == 0 {
		localLines = append(localLines, dimStyle.Render("  <empty>"))
	} else {
		displayCount := 0
		maxDisplay := 4
		for i, f := range m.localFiles {
			if displayCount >= maxDisplay {
				remaining := len(m.localFiles) - maxDisplay
				localLines = append(localLines, dimStyle.Render(fmt.Sprintf("  ... +%d more", remaining)))
				break
			}
			clean := strings.TrimSpace(f)
			displayName := clean
			if len(f) > panelWidth-10 {
				displayName = f[:panelWidth-13] + "..."
			}
			localLines = append(localLines, normalStyle.Render(fmt.Sprintf("  [%d] %s", i+1, displayName)))
			displayCount++
		}
	}
	
	for len(localLines) < panelHeight {
		localLines = append(localLines, "")
	}

	// Panel Derecho - Archivos Remotos
	var remoteLines []string
	remoteLines = append(remoteLines, infoStyle.Render("REMOTE FILES"))
	remoteLines = append(remoteLines, dimStyle.Render(strings.Repeat("─", panelWidth-4)))

	if len(m.remoteFiles) == 0 {
			remoteLines = append(remoteLines, dimStyle.Render(" <use LIST to refresh>"))
	} else {
			displayCount := 0
			maxDisplay := 4
			for i, f := range m.remoteFiles {
					if displayCount >= maxDisplay {
							remaining := len(m.remoteFiles) - maxDisplay
							remoteLines = append(remoteLines, dimStyle.Render(fmt.Sprintf("  ... +%d more", remaining)))
							break
					}
					clean := strings.TrimSpace(f)
					displayName := clean
					if len(clean) > panelWidth-10 {
							displayName = clean[:panelWidth-13] + "..."
					}else{
						displayName = clean
					}
					remoteLines = append(remoteLines, normalStyle.Render(fmt.Sprintf("  [%d] %s", i+1, displayName)))
					displayCount++
			}
	}

	for len(remoteLines) < panelHeight {
			remoteLines = append(remoteLines, "")
	}

	// Construir paneles side-by-side
	localPanel := panelBorderStyle.Width(panelWidth).Height(panelHeight).Render(strings.Join(localLines, "\n"))
	remotePanel := panelBorderStyle.Width(panelWidth).Height(panelHeight).Render(strings.Join(remoteLines, "\n"))

	return lipgloss.JoinHorizontal(lipgloss.Top, localPanel, remotePanel)


}

func (m model) renderInput() string {
	var s strings.Builder

	title := "> INFO REQUEST"
	if m.selectedCmd == "get" {
		title = "> DOWNLOAD REQUEST"
	}else if m.selectedCmd == "cat" {
		title = "> CAT REQUEST"
	}
	s.WriteString("\n")
	s.WriteString(normalStyle.Render( title ))
	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) )
	s.WriteString( "\n\n")

	s.WriteString(normalStyle.Render("  Filename: "))
	s.WriteString(m.input.View())
	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) + "\n")
	s.WriteString(helpStyle.Render("  [ENTER] Confirm  [ESC] Cancel"))

	return s.String()
}

func (m model) renderCat() string {
	var s strings.Builder
	
	s.WriteString("\n")
	s.WriteString(normalStyle.Render("> FILE CONTENT"))
	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)))
	s.WriteString("\n\n")

	if m.catContent == "" {
		s.WriteString(errorStyle.Render("  ERROR: No content available\n"))
	} else {
		lines := strings.Split(m.catContent, "\n")
		maxVisible := 15
		start := m.catOffset
		end := start + maxVisible
		if end > len(lines) {
			end = len(lines)
		}

		for i := start; i < end; i++ {
			line := lines[i]
			// Reemplazar tabs por espacios
			line = strings.ReplaceAll(line, "\t", "    ")
			// Truncar líneas muy largas (usando rune count para caracteres Unicode)
			runes := []rune(line)
			if len(runes) > 68 {
				line = string(runes[:65]) + "..."
			}
			s.WriteString(normalStyle.Render(fmt.Sprintf("%4d | %s", i+1, line)))
			s.WriteString("\n")
		}

		if len(lines) > maxVisible {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("  Showing lines %d-%d of %d", start+1, end, len(lines))))
			s.WriteString("\n")
		}
		
		s.WriteString("\n")
		s.WriteString(infoStyle.Render(fmt.Sprintf("  Total size: %d bytes, %d lines", len(m.catContent), len(lines))))
	}

	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) + "\n")
	s.WriteString(helpStyle.Render("  [↑/↓] Scroll  [PgUp/PgDn] Fast scroll  [ENTER/ESC] Return to menu"))

	return s.String()
}

func (m model) renderList() string {
	var s strings.Builder
	s.WriteString("\n\n")
	s.WriteString(normalStyle.Render("> SELECT FILE TO UPLOAD"))
	s.WriteString("\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) )
	s.WriteString("\n\n")

	if len(m.localFiles) == 0 {
		s.WriteString(errorStyle.Render("  ERROR: No files found in ./local_files/\n"))
	} else {
		maxVisible := 10
		start := m.listOffset
		end := start + maxVisible
		if end > len(m.localFiles) {
			end = len(m.localFiles)
		}

		for i := start; i < end; i++ {
			f := m.localFiles[i]
			displayName := f
			if len(f) > 70 {
				displayName = f[:67] + "..."
			}

			if m.cursor == i {
				s.WriteString(selectedStyle.Render(fmt.Sprintf("> [%02d] %s", i+1, displayName)) + "\n")
			} else {
				s.WriteString(normalStyle.Render(fmt.Sprintf("  [%02d] %s", i+1, displayName)) + "\n")
			}
		}

		if len(m.localFiles) > maxVisible {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("  Showing %d-%d of %d files", start+1, end, len(m.localFiles))))
		}
	}

	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) + "\n")
	s.WriteString(helpStyle.Render("  [↑/↓] Navigate  [ENTER] Upload  [ESC] Cancel"))

	return s.String()
}

func (m model) renderInfo() string {
	var s strings.Builder
	s.WriteString("\n")
	s.WriteString(normalStyle.Render("> FILE INFORMATION"))
	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) )
	s.WriteString("\n\n")

	if len(m.fileInfo) == 0 {
		s.WriteString(errorStyle.Render("  ERROR: No information available\n"))
	} else {
		s.WriteString(infoStyle.Render(fmt.Sprintf("  Total Blocks: %d", len(m.fileInfo))))
		s.WriteString("\n\n")

		maxVisible := 8
		start := m.listOffset
		end := start + maxVisible
		if end > len(m.fileInfo) {
			end = len(m.fileInfo)
		}

		for i := start; i < end; i++ {
			lbl := m.fileInfo[i]
			
			if m.cursor == i {
				s.WriteString(selectedStyle.Render(fmt.Sprintf("> Block ID: %s", lbl.Block)) + "\n")
				for j, addr := range lbl.Node_address {
					s.WriteString(selectedStyle.Render(fmt.Sprintf("    ├─ Replica %d: %s", j+1, addr)) + "\n")
				}
			} else {
				s.WriteString(normalStyle.Render(fmt.Sprintf("  Block ID: %s", lbl.Block)) + "\n")
				for j, addr := range lbl.Node_address {
					s.WriteString(normalStyle.Render(fmt.Sprintf("    ├─ Replica %d: %s", j+1, addr)) + "\n")
				}
			}
			s.WriteString("\n")
		}

		if len(m.fileInfo) > maxVisible {
			s.WriteString(dimStyle.Render(fmt.Sprintf("  Showing %d-%d of %d blocks\n", start+1, end, len(m.fileInfo))))
		}
	}

	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) + "\n")
	s.WriteString(helpStyle.Render("  [↑/↓] Navigate  [ENTER/ESC] Return to menu"))

	return s.String()
}

func (m model) renderLogger() string {
	var s strings.Builder

	s.WriteString("\n")
	s.WriteString(normalStyle.Render("> OPERATION LOGGER"))
	s.WriteString("\n\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) )
	s.WriteString("\n\n")

	if len(m.logs) == 0 {
		s.WriteString(dimStyle.Render("  No logs available\n"))
	} else {
		maxVisible := 15
		start := m.logOffset
		end := start + maxVisible
		if end > len(m.logs) {
			end = len(m.logs)
		}

		for i := start; i < end; i++ {
			log := m.logs[i]
			timestamp := log.timestamp.Format("15:04:05")
			
			var style lipgloss.Style
			var prefix string
			switch log.logType {
			case "error":
				style = errorStyle
				prefix = "[ERR]"
			case "success":
				style = successStyle
				prefix = "[OK ]"
			case "warning":
				style = warningStyle
				prefix = "[WRN]"
			default:
				style = normalStyle
				prefix = "[INF]"
			}
			
			s.WriteString(dimStyle.Render(fmt.Sprintf("  %s ", timestamp)))
			s.WriteString(style.Render(fmt.Sprintf("%s %s", prefix, log.message)))
			s.WriteString("\n")
		}

		if len(m.logs) > maxVisible {
			s.WriteString("\n")
			s.WriteString(dimStyle.Render(fmt.Sprintf("  Showing %d-%d of %d logs", start+1, end, len(m.logs))))
		}
	}

	s.WriteString("\n")
	s.WriteString(dimStyle.Render(strings.Repeat("─", 79)) + "\n")
	s.WriteString(helpStyle.Render("  [↑/↓] Scroll  [ENTER/ESC] Return to menu"))

	return s.String()
}

func Run(dfsClient *client.DFSClient) error {
	p := tea.NewProgram(initialModel(dfsClient), tea.WithAltScreen())
	if _, err := p.Run(); err != nil {
		return fmt.Errorf("error ejecutando CLI: %w", err)
	}
	return nil
}
