use std::{collections::HashMap, process::Command, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::external_tools::{ExternalTool, ExternalToolRegistry, ExternalToolResult};

#[cfg(feature = "hardware")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "hardware")]
use tokio::sync::Mutex;

pub const FORM_ZERO_PERIPHERALS_JSON_ENV: &str = "FORM_ZERO_PERIPHERALS_JSON";

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct PeripheralsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub boards: Vec<PeripheralBoardConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datasheet_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PeripheralBoardConfig {
    pub board: String,
    #[serde(default = "default_peripheral_transport")]
    pub transport: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default = "default_peripheral_baud")]
    pub baud: u32,
}

fn default_peripheral_transport() -> String {
    "serial".to_string()
}

fn default_peripheral_baud() -> u32 {
    115_200
}

impl Default for PeripheralBoardConfig {
    fn default() -> Self {
        Self {
            board: String::new(),
            transport: default_peripheral_transport(),
            path: None,
            baud: default_peripheral_baud(),
        }
    }
}

pub fn peripherals_config_from_env() -> PeripheralsConfig {
    std::env::var(FORM_ZERO_PERIPHERALS_JSON_ENV)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

pub async fn build_external_tool_registry(
    config: PeripheralsConfig,
) -> Result<ExternalToolRegistry> {
    let runtime = Arc::new(PeripheralsRuntime::new(config).await?);
    let tools: Vec<Arc<dyn ExternalTool>> = vec![
        Arc::new(GpioReadTool::new(runtime.clone())),
        Arc::new(GpioWriteTool::new(runtime.clone())),
        Arc::new(SerialQueryTool::new(runtime.clone())),
        Arc::new(SerialWriteTool::new(runtime.clone())),
        Arc::new(HardwareCapabilitiesTool::new(runtime.clone())),
        Arc::new(HardwareBoardInfoTool::new(runtime.clone())),
        Arc::new(HardwareMemoryMapTool::new(runtime.clone())),
        Arc::new(HardwareMemoryReadTool::new(runtime.clone())),
        Arc::new(FlashFirmwareTool::new(runtime.clone())),
        Arc::new(ArduinoUploadTool::new(runtime)),
    ];
    Ok(ExternalToolRegistry::new(tools))
}

#[derive(Clone)]
struct PeripheralsRuntime {
    config: Arc<PeripheralsConfig>,
    #[cfg(feature = "hardware")]
    serial_transports: Arc<HashMap<String, Arc<SerialTransport>>>,
    serial_errors: Arc<HashMap<String, String>>,
}

impl PeripheralsRuntime {
    async fn new(config: PeripheralsConfig) -> Result<Self> {
        #[cfg(feature = "hardware")]
        let mut serial_transports = HashMap::new();
        let mut serial_errors = HashMap::new();

        if config.enabled {
            #[cfg(feature = "hardware")]
            for board in &config.boards {
                if board.transport != "serial" {
                    continue;
                }
                let Some(path) = board.path.as_deref() else {
                    serial_errors.insert(
                        board.board.clone(),
                        "serial board requires `path` in peripherals config".to_string(),
                    );
                    continue;
                };
                match SerialTransport::connect(path, board.baud).await {
                    Ok(transport) => {
                        serial_transports.insert(board.board.clone(), Arc::new(transport));
                    }
                    Err(error) => {
                        serial_errors.insert(board.board.clone(), error.to_string());
                    }
                }
            }
        }

        #[cfg(not(feature = "hardware"))]
        for board in &config.boards {
            if board.transport == "serial" {
                serial_errors.insert(
                    board.board.clone(),
                    "serial peripherals require the `hardware` Cargo feature".to_string(),
                );
            }
        }

        Ok(Self {
            config: Arc::new(config),
            #[cfg(feature = "hardware")]
            serial_transports: Arc::new(serial_transports),
            serial_errors: Arc::new(serial_errors),
        })
    }

    fn has_configured_boards(&self) -> bool {
        self.config.enabled && !self.config.boards.is_empty()
    }

    fn board_config_by_name(&self, board_name: &str) -> Option<&PeripheralBoardConfig> {
        self.config
            .boards
            .iter()
            .find(|board| normalize_token(&board.board) == normalize_token(board_name))
    }

    fn resolve_board_name(
        &self,
        board_arg: Option<&str>,
        device_selector: Option<&str>,
        transport_hint: Option<&str>,
    ) -> Result<String> {
        let configured = self
            .config
            .boards
            .iter()
            .filter(|board| {
                transport_hint
                    .map(|transport| {
                        normalize_token(&board.transport) == normalize_token(transport)
                    })
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        if configured.is_empty() {
            bail!(
                "no configured peripherals matched the request; set {} to enable board routing",
                FORM_ZERO_PERIPHERALS_JSON_ENV
            );
        }

        let requested = board_arg
            .map(ToOwned::to_owned)
            .or_else(|| device_selector.and_then(|selector| device_selector_board_hint(selector)));

        let Some(requested) = requested else {
            return Ok(configured[0].board.clone());
        };
        let requested = normalize_token(&requested);

        if let Some(board) = configured
            .iter()
            .find(|board| normalize_token(&board.board) == requested)
        {
            return Ok(board.board.clone());
        }
        if let Some(board) = configured.iter().find(|board| {
            board_aliases(&board.board)
                .iter()
                .any(|alias| alias == &requested)
        }) {
            return Ok(board.board.clone());
        }
        if let Some(board) = configured
            .iter()
            .find(|board| normalize_token(&board.board).contains(&requested))
        {
            return Ok(board.board.clone());
        }

        bail!("unable to resolve board `{requested}` from configured peripherals")
    }

    #[cfg(feature = "hardware")]
    fn serial_transport_for_board(&self, board_name: &str) -> Result<Arc<SerialTransport>> {
        if let Some(transport) = self.serial_transports.get(board_name) {
            return Ok(transport.clone());
        }
        if let Some(error) = self.serial_errors.get(board_name) {
            bail!("{error}");
        }
        bail!("serial transport for board `{board_name}` is not available")
    }

    #[cfg(not(feature = "hardware"))]
    fn serial_transport_for_board(&self, board_name: &str) -> Result<()> {
        if let Some(error) = self.serial_errors.get(board_name) {
            bail!("{error}");
        }
        bail!("serial peripherals require the `hardware` Cargo feature")
    }

    fn ensure_enabled(&self) -> Result<()> {
        if self.has_configured_boards() {
            Ok(())
        } else {
            bail!(
                "no peripherals configured; set {} with enabled boards",
                FORM_ZERO_PERIPHERALS_JSON_ENV
            )
        }
    }

    async fn execute_gpio_read(&self, args: Value) -> Result<ExternalToolResult> {
        self.ensure_enabled()?;
        let pin = args
            .get("pin")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("gpio_read requires integer field `pin`"))?;
        let board_name = self.resolve_board_name(
            args.get("board").and_then(Value::as_str),
            args.get("device_selector").and_then(Value::as_str),
            args.get("transport").and_then(Value::as_str),
        )?;
        let board = self
            .board_config_by_name(&board_name)
            .ok_or_else(|| anyhow!("missing board configuration for `{board_name}`"))?;

        match board.transport.as_str() {
            "serial" => {
                #[cfg(feature = "hardware")]
                {
                    return self
                        .serial_transport_for_board(&board_name)?
                        .request_json("gpio_read", json!({ "pin": pin }))
                        .await;
                }
                #[cfg(not(feature = "hardware"))]
                {
                    let _ = pin;
                    self.serial_transport_for_board(&board_name)?;
                    unreachable!();
                }
            }
            "bridge" => bridge_gpio_read(pin).await,
            "native" => native_gpio_read(board, pin).await,
            other => bail!("unsupported transport `{other}` for gpio_read"),
        }
    }

    async fn execute_gpio_write(&self, args: Value) -> Result<ExternalToolResult> {
        self.ensure_enabled()?;
        let pin = args
            .get("pin")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("gpio_write requires integer field `pin`"))?;
        let value = args
            .get("value")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("gpio_write requires integer field `value`"))?;
        let board_name = self.resolve_board_name(
            args.get("board").and_then(Value::as_str),
            args.get("device_selector").and_then(Value::as_str),
            args.get("transport").and_then(Value::as_str),
        )?;
        let board = self
            .board_config_by_name(&board_name)
            .ok_or_else(|| anyhow!("missing board configuration for `{board_name}`"))?;

        match board.transport.as_str() {
            "serial" => {
                #[cfg(feature = "hardware")]
                {
                    return self
                        .serial_transport_for_board(&board_name)?
                        .request_json("gpio_write", json!({ "pin": pin, "value": value }))
                        .await;
                }
                #[cfg(not(feature = "hardware"))]
                {
                    let _ = (pin, value);
                    self.serial_transport_for_board(&board_name)?;
                    unreachable!();
                }
            }
            "bridge" => bridge_gpio_write(pin, value).await,
            "native" => native_gpio_write(board, pin, value).await,
            other => bail!("unsupported transport `{other}` for gpio_write"),
        }
    }

    async fn execute_serial_query(&self, args: Value) -> Result<ExternalToolResult> {
        self.ensure_enabled()?;
        let board_name = self.resolve_board_name(
            args.get("board").and_then(Value::as_str),
            args.get("device_selector").and_then(Value::as_str),
            Some("serial"),
        )?;
        #[cfg(feature = "hardware")]
        {
            let transport = self.serial_transport_for_board(&board_name)?;
            if let Some(command) = args.get("command").and_then(Value::as_str) {
                let command_args = args
                    .get("args")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default()));
                return transport.request_json(command, command_args).await;
            }
            let line = args
                .get("line")
                .or_else(|| args.get("query"))
                .or_else(|| args.get("payload"))
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("serial_query requires `command` or string field `line`"))?;
            return transport.query_raw_line(line).await;
        }
        #[cfg(not(feature = "hardware"))]
        {
            let _ = args;
            self.serial_transport_for_board(&board_name)?;
            unreachable!();
        }
    }

    async fn execute_serial_write(&self, args: Value) -> Result<ExternalToolResult> {
        self.ensure_enabled()?;
        let board_name = self.resolve_board_name(
            args.get("board").and_then(Value::as_str),
            args.get("device_selector").and_then(Value::as_str),
            Some("serial"),
        )?;
        #[cfg(feature = "hardware")]
        {
            let transport = self.serial_transport_for_board(&board_name)?;
            if let Some(command) = args.get("command").and_then(Value::as_str) {
                let command_args = args
                    .get("args")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default()));
                return transport.request_json(command, command_args).await;
            }
            let line = args
                .get("line")
                .or_else(|| args.get("payload"))
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("serial_write requires `command` or string field `line`"))?;
            return transport.write_raw_line(line).await;
        }
        #[cfg(not(feature = "hardware"))]
        {
            let _ = args;
            self.serial_transport_for_board(&board_name)?;
            unreachable!();
        }
    }

    async fn execute_capabilities(&self, args: Value) -> Result<ExternalToolResult> {
        if !self.has_configured_boards() {
            return Ok(ExternalToolResult {
                success: false,
                output: String::new(),
                error: Some("No peripherals configured.".to_string()),
            });
        }

        let requested_board = args.get("board").and_then(Value::as_str);
        let mut outputs = Vec::new();

        for board in &self.config.boards {
            if let Some(requested_board) = requested_board {
                if normalize_token(&board.board) != normalize_token(requested_board) {
                    continue;
                }
            }
            match board.transport.as_str() {
                "serial" => {
                    #[cfg(feature = "hardware")]
                    match self
                        .serial_transport_for_board(&board.board)?
                        .capabilities()
                        .await
                    {
                        Ok(result) => {
                            outputs.push(format_capabilities_output(&board.board, result))
                        }
                        Err(error) => outputs.push(format!("{}: error - {}", board.board, error)),
                    }
                    #[cfg(not(feature = "hardware"))]
                    if let Some(error) = self.serial_errors.get(&board.board) {
                        outputs.push(format!("{}: {}", board.board, error));
                    }
                }
                "native" => outputs.push(format!(
                    "{}: gpio bcm available, transport=native",
                    board.board
                )),
                "bridge" => outputs.push(format!(
                    "{}: gpio bridge available, transport=bridge",
                    board.board
                )),
                other => outputs.push(format!("{}: unsupported transport {}", board.board, other)),
            }
        }

        let output = if outputs.is_empty() {
            "No matching board or capabilities not supported.".to_string()
        } else {
            outputs.join("\n")
        };
        Ok(ExternalToolResult {
            success: !outputs.is_empty(),
            output,
            error: None,
        })
    }

    async fn execute_board_info(&self, args: Value) -> Result<ExternalToolResult> {
        let board_name = args
            .get("board")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.config.boards.first().map(|board| board.board.clone()))
            .unwrap_or_else(|| "unknown".to_string());

        if !self.has_configured_boards() && args.get("board").is_none() {
            return Ok(ExternalToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "No peripherals configured. Provide `board` or configure peripherals."
                        .to_string(),
                ),
            });
        }

        #[cfg(feature = "probe")]
        if board_name == "nucleo-f401re" || board_name == "nucleo-f411re" {
            let chip = if board_name == "nucleo-f411re" {
                "STM32F411RETx"
            } else {
                "STM32F401RETx"
            };
            if let Ok(info) = probe_board_info(chip) {
                return Ok(ExternalToolResult {
                    success: true,
                    output: info,
                    error: None,
                });
            }
        }

        let mut output = static_info_for_board(&board_name)
            .unwrap_or_else(|| format!("Board `{board_name}` has no bundled static info."));
        if let Some(memory_map) = static_memory_map_for_board(&board_name) {
            output.push_str("\n\n**Memory map:**\n");
            output.push_str(memory_map);
        }
        Ok(ExternalToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn execute_memory_map(&self, args: Value) -> Result<ExternalToolResult> {
        let board_name = args
            .get("board")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| self.config.boards.first().map(|board| board.board.clone()))
            .unwrap_or_else(|| "unknown".to_string());

        #[cfg(feature = "probe")]
        if board_name == "nucleo-f401re" || board_name == "nucleo-f411re" {
            let chip = if board_name == "nucleo-f411re" {
                "STM32F411RETx"
            } else {
                "STM32F401RETx"
            };
            if let Ok(output) = probe_memory_map(chip) {
                return Ok(ExternalToolResult {
                    success: true,
                    output: format!("**{}** (via probe-rs):\n{}", board_name, output),
                    error: None,
                });
            }
        }

        let output = static_memory_map_for_board(&board_name)
            .map(|memory_map| format!("**{}** (static):\n{}", board_name, memory_map))
            .unwrap_or_else(|| format!("No memory map for board `{}`.", board_name));
        Ok(ExternalToolResult {
            success: true,
            output,
            error: None,
        })
    }

    async fn execute_memory_read(&self, args: Value) -> Result<ExternalToolResult> {
        let board_name = args
            .get("board")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                args.get("device_selector")
                    .and_then(Value::as_str)
                    .and_then(device_selector_board_hint)
            })
            .or_else(|| self.config.boards.first().map(|board| board.board.clone()))
            .unwrap_or_else(|| "unknown".to_string());
        #[cfg(not(feature = "probe"))]
        let _ = &board_name;
        let address = args
            .get("address")
            .and_then(Value::as_str)
            .and_then(parse_hex_address)
            .unwrap_or(0x2000_0000);
        let length = args
            .get("length")
            .and_then(Value::as_u64)
            .unwrap_or(128)
            .clamp(1, 256) as usize;

        #[cfg(feature = "probe")]
        {
            let Some(chip) = chip_for_board(&board_name) else {
                bail!("hardware_memory_read only supports Nucleo STM32 boards");
            };
            let output = probe_read_memory(chip, address, length)?;
            return Ok(ExternalToolResult {
                success: true,
                output,
                error: None,
            });
        }

        #[cfg(not(feature = "probe"))]
        {
            let _ = (address, length);
            Ok(ExternalToolResult {
                success: false,
                output: String::new(),
                error: Some("hardware_memory_read requires the `probe` Cargo feature.".to_string()),
            })
        }
    }

    async fn execute_flash_firmware(&self, args: Value) -> Result<ExternalToolResult> {
        let board_name = args
            .get("board")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                args.get("device_selector")
                    .and_then(Value::as_str)
                    .and_then(device_selector_board_hint)
            })
            .or_else(|| self.config.boards.first().map(|board| board.board.clone()))
            .ok_or_else(|| anyhow!("flash_firmware requires `board` or configured peripherals"))?;

        if board_name.starts_with("nucleo-") {
            let firmware_path = args
                .get("firmware_path")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("flash_firmware for Nucleo requires `firmware_path`"))?;
            flash_nucleo_binary(&board_name, firmware_path)?;
            return Ok(ExternalToolResult {
                success: true,
                output: format!("Flashed {} from {}", board_name, firmware_path),
                error: None,
            });
        }

        if board_name == "arduino-uno" {
            return self.execute_arduino_upload(args).await;
        }

        bail!("flash_firmware is not implemented for board `{board_name}`")
    }

    async fn execute_arduino_upload(&self, args: Value) -> Result<ExternalToolResult> {
        let board_name = args
            .get("board")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.config
                    .boards
                    .iter()
                    .find(|board| board.board == "arduino-uno")
                    .map(|board| board.board.clone())
            })
            .unwrap_or_else(|| "arduino-uno".to_string());
        let code = args
            .get("code")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("arduino_upload requires string field `code`"))?;
        let port = args
            .get("port")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| {
                self.board_config_by_name(&board_name)
                    .and_then(|board| board.path.clone())
            })
            .ok_or_else(|| anyhow!("arduino_upload requires `port` or configured board path"))?;

        arduino_upload_code(&port, code).await
    }
}

#[cfg(feature = "hardware")]
fn format_capabilities_output(board_name: &str, result: ExternalToolResult) -> String {
    if !result.success {
        return format!(
            "{}: {}",
            board_name,
            result.error.unwrap_or_else(|| "unknown error".to_string())
        );
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(&result.output) {
        format!(
            "{}: gpio {:?}, led_pin {:?}",
            board_name,
            parsed.get("gpio").unwrap_or(&json!([])),
            parsed.get("led_pin").unwrap_or(&json!(null))
        )
    } else {
        format!("{}: {}", board_name, result.output)
    }
}

fn normalize_token(value: &str) -> String {
    value
        .to_ascii_lowercase()
        .chars()
        .map(|character| match character {
            '_' | '/' | ':' | '.' => '-',
            other => other,
        })
        .collect::<String>()
}

fn board_aliases(board_name: &str) -> Vec<String> {
    let normalized = normalize_token(board_name);
    let mut aliases = vec![normalized.clone()];
    if normalized.starts_with("nucleo-") {
        aliases.push("nucleo".to_string());
    }
    if normalized.starts_with("arduino-uno-q") || normalized == "uno-q" {
        aliases.push("uno-q".to_string());
        aliases.push("unoq".to_string());
    }
    if normalized.starts_with("arduino-uno") {
        aliases.push("arduino".to_string());
        aliases.push("uno".to_string());
    }
    if normalized.starts_with("rpi-") || normalized.contains("raspberry") {
        aliases.push("rpi".to_string());
        aliases.push("raspberry-pi".to_string());
        aliases.push("raspberry".to_string());
    }
    aliases.sort();
    aliases.dedup();
    aliases
}

fn device_selector_board_hint(device_selector: &str) -> Option<String> {
    let selector = device_selector;
    let selector = selector.strip_prefix("board:").unwrap_or(selector);
    let board = selector.split('/').next().unwrap_or(selector).trim();
    if board.is_empty() {
        None
    } else {
        Some(board.to_string())
    }
}

fn static_info_for_board(board: &str) -> Option<String> {
    const BOARD_INFO: &[(&str, &str, &str)] = &[
        (
            "nucleo-f401re",
            "STM32F401RET6",
            "ARM Cortex-M4, 84 MHz. Flash: 512 KB, RAM: 128 KB. User LED on PA5 (pin 13).",
        ),
        (
            "nucleo-f411re",
            "STM32F411RET6",
            "ARM Cortex-M4, 100 MHz. Flash: 512 KB, RAM: 128 KB. User LED on PA5 (pin 13).",
        ),
        (
            "arduino-uno",
            "ATmega328P",
            "8-bit AVR, 16 MHz. Flash: 16 KB, SRAM: 2 KB. Built-in LED on pin 13.",
        ),
        (
            "arduino-uno-q",
            "STM32U585 + Qualcomm",
            "Dual-core: STM32 (MCU) + Linux (aarch64). GPIO via Bridge app on port 9999.",
        ),
        (
            "esp32",
            "ESP32",
            "Dual-core Xtensa LX6, 240 MHz. Flash: 4 MB typical. Built-in LED on GPIO 2.",
        ),
        (
            "rpi-gpio",
            "Raspberry Pi",
            "ARM Linux. Native GPIO via rppal. No fixed LED pin.",
        ),
    ];

    BOARD_INFO
        .iter()
        .find(|(known_board, _, _)| *known_board == board)
        .map(|(_, chip, description)| {
            format!(
                "**Board:** {}\n**Chip:** {}\n**Description:** {}",
                board, chip, description
            )
        })
}

fn static_memory_map_for_board(board: &str) -> Option<&'static str> {
    match board {
        "nucleo-f401re" | "nucleo-f411re" => Some(
            "Flash: 0x0800_0000 - 0x0807_FFFF (512 KB)\nRAM: 0x2000_0000 - 0x2001_FFFF (128 KB)",
        ),
        "arduino-uno" => Some("Flash: 0x0000 - 0x3FFF (16 KB)\nSRAM: 0x0100 - 0x08FF (2 KB)\nEEPROM: 0x0000 - 0x03FF (1 KB)"),
        "arduino-mega" => Some("Flash: 0x0000 - 0x3FFFF (256 KB)\nSRAM: 0x0200 - 0x21FF (8 KB)\nEEPROM: 0x0000 - 0x0FFF (4 KB)"),
        "esp32" => Some("Flash: 0x3F40_0000 - 0x3F7F_FFFF (4 MB typical)\nIRAM: 0x4000_0000 - 0x4005_FFFF\nDRAM: 0x3FFB_0000 - 0x3FFF_FFFF"),
        _ => None,
    }
}

fn chip_for_board(board: &str) -> Option<&'static str> {
    match board {
        "nucleo-f401re" => Some("STM32F401RETx"),
        "nucleo-f411re" => Some("STM32F411RETx"),
        _ => None,
    }
}

fn parse_hex_address(value: &str) -> Option<u64> {
    let value = value
        .trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X");
    u64::from_str_radix(value, 16).ok()
}

#[cfg(feature = "probe")]
fn probe_board_info(chip: &str) -> Result<String> {
    use probe_rs::config::MemoryRegion;
    use probe_rs::{Session, SessionConfig};

    let session = Session::auto_attach(chip, SessionConfig::default())
        .map_err(|error| anyhow!("{}", error))?;
    let target = session.target();
    let architecture = session.architecture();

    let mut output = format!(
        "**Board:** {}\n**Chip:** {}\n**Architecture:** {:?}\n\n**Memory map:**\n",
        chip, target.name, architecture
    );
    for region in target.memory_map.iter() {
        match region {
            MemoryRegion::Ram(ram) => {
                let (start, end) = (ram.range.start, ram.range.end);
                output.push_str(&format!(
                    "RAM: 0x{:08X} - 0x{:08X} ({} KB)\n",
                    start,
                    end,
                    (end - start) / 1024
                ));
            }
            MemoryRegion::Nvm(flash) => {
                let (start, end) = (flash.range.start, flash.range.end);
                output.push_str(&format!(
                    "Flash: 0x{:08X} - 0x{:08X} ({} KB)\n",
                    start,
                    end,
                    (end - start) / 1024
                ));
            }
            _ => {}
        }
    }
    output.push_str("\n(Info read via USB/SWD — no firmware on target needed.)");
    Ok(output)
}

#[cfg(feature = "probe")]
fn probe_memory_map(chip: &str) -> Result<String> {
    use probe_rs::config::MemoryRegion;
    use probe_rs::{Session, SessionConfig};

    let session = Session::auto_attach(chip, SessionConfig::default())
        .map_err(|error| anyhow!("probe-rs attach failed: {}", error))?;
    let target = session.target();
    let mut output = String::new();
    for region in target.memory_map.iter() {
        match region {
            MemoryRegion::Ram(ram) => {
                let start = ram.range.start;
                let end = ram.range.end;
                output.push_str(&format!(
                    "RAM: 0x{:08X} - 0x{:08X} ({} KB)\n",
                    start,
                    end,
                    (end - start) / 1024
                ));
            }
            MemoryRegion::Nvm(flash) => {
                let start = flash.range.start;
                let end = flash.range.end;
                output.push_str(&format!(
                    "Flash: 0x{:08X} - 0x{:08X} ({} KB)\n",
                    start,
                    end,
                    (end - start) / 1024
                ));
            }
            _ => {}
        }
    }
    if output.is_empty() {
        output = "Could not read memory regions from probe.".to_string();
    }
    Ok(output)
}

#[cfg(feature = "probe")]
fn probe_read_memory(chip: &str, address: u64, length: usize) -> Result<String> {
    use probe_rs::MemoryInterface;
    use probe_rs::{Session, SessionConfig};

    let mut session = Session::auto_attach(chip, SessionConfig::default())
        .map_err(|error| anyhow!("{}", error))?;
    let mut core = session.core(0)?;
    let mut buffer = vec![0u8; length];
    core.read_8(address, &mut buffer)
        .map_err(|error| anyhow!("{}", error))?;

    let mut output = format!("Memory read from 0x{:08X} ({} bytes):\n\n", address, length);
    const COLUMNS: usize = 16;
    for (index, chunk) in buffer.chunks(COLUMNS).enumerate() {
        let line_address = address + (index * COLUMNS) as u64;
        let hex = chunk
            .iter()
            .map(|byte| format!("{:02X}", byte))
            .collect::<Vec<_>>()
            .join(" ");
        let ascii = chunk
            .iter()
            .map(|byte| {
                if byte.is_ascii_graphic() || *byte == b' ' {
                    *byte as char
                } else {
                    '.'
                }
            })
            .collect::<String>();
        output.push_str(&format!("0x{:08X}  {:48}  {}\n", line_address, hex, ascii));
    }
    Ok(output)
}

fn probe_rs_available() -> bool {
    Command::new("probe-rs")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn flash_nucleo_binary(board_name: &str, firmware_path: &str) -> Result<()> {
    let chip = chip_for_board(board_name)
        .ok_or_else(|| anyhow!("unsupported Nucleo board `{board_name}` for flashing"))?;
    if !probe_rs_available() {
        bail!(
            "probe-rs not found. Install probe-rs-tools before flashing `{}`.",
            board_name
        );
    }
    let output = Command::new("probe-rs")
        .args(["run", "--chip", chip, firmware_path])
        .output()
        .map_err(|error| anyhow!("failed to spawn probe-rs: {error}"))?;
    if !output.status.success() {
        bail!(
            "probe-rs flash failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

async fn arduino_upload_code(port: &str, code: &str) -> Result<ExternalToolResult> {
    if code.trim().is_empty() {
        return Ok(ExternalToolResult {
            success: false,
            output: String::new(),
            error: Some("Code cannot be empty".to_string()),
        });
    }
    if Command::new("arduino-cli").arg("version").output().is_err() {
        return Ok(ExternalToolResult {
            success: false,
            output: String::new(),
            error: Some("arduino-cli not found.".to_string()),
        });
    }

    let sketch_name = "form_zero_sketch";
    let temp_dir = std::env::temp_dir().join(format!("form_zero_{}", Uuid::new_v4()));
    let sketch_dir = temp_dir.join(sketch_name);
    let ino_path = sketch_dir.join(format!("{}.ino", sketch_name));
    tokio::fs::create_dir_all(&sketch_dir).await?;
    tokio::fs::write(&ino_path, code).await?;

    let sketch_path = sketch_dir.to_string_lossy().to_string();
    let fqbn = "arduino:avr:uno";
    let compile = Command::new("arduino-cli")
        .args(["compile", "--fqbn", fqbn, &sketch_path])
        .output()
        .map_err(|error| anyhow!("arduino-cli compile failed: {error}"))?;
    if !compile.status.success() {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Ok(ExternalToolResult {
            success: false,
            output: format!(
                "Compile failed:\n{}",
                String::from_utf8_lossy(&compile.stderr)
            ),
            error: Some("Arduino compile error".to_string()),
        });
    }

    let upload = Command::new("arduino-cli")
        .args(["upload", "-p", port, "--fqbn", fqbn, &sketch_path])
        .output()
        .map_err(|error| anyhow!("arduino-cli upload failed: {error}"))?;
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    if !upload.status.success() {
        return Ok(ExternalToolResult {
            success: false,
            output: format!(
                "Upload failed:\n{}",
                String::from_utf8_lossy(&upload.stderr)
            ),
            error: Some("Arduino upload error".to_string()),
        });
    }

    Ok(ExternalToolResult {
        success: true,
        output: "Sketch compiled and uploaded successfully.".to_string(),
        error: None,
    })
}

#[cfg(feature = "hardware")]
const ALLOWED_SERIAL_PATH_PREFIXES: &[&str] = &[
    "/dev/ttyACM",
    "/dev/ttyUSB",
    "/dev/tty.usbmodem",
    "/dev/cu.usbmodem",
    "/dev/tty.usbserial",
    "/dev/cu.usbserial",
    "COM",
];

#[cfg(feature = "hardware")]
fn is_path_allowed(path: &str) -> bool {
    ALLOWED_SERIAL_PATH_PREFIXES
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

#[cfg(feature = "hardware")]
struct SerialTransport {
    port: Mutex<tokio_serial::SerialStream>,
}

#[cfg(feature = "hardware")]
impl SerialTransport {
    async fn connect(path: &str, baud: u32) -> Result<Self> {
        use tokio_serial::SerialPortBuilderExt;

        if !is_path_allowed(path) {
            bail!("serial path `{}` is not allowed", path);
        }
        let port = tokio_serial::new(path, baud)
            .open_native_async()
            .map_err(|error| anyhow!("failed to open {}: {}", path, error))?;
        Ok(Self {
            port: Mutex::new(port),
        })
    }

    async fn request_json(&self, command: &str, args: Value) -> Result<ExternalToolResult> {
        let response = self
            .with_timeout(send_json_request(&self.port, command, args))
            .await?;
        Ok(parse_serial_tool_result(response))
    }

    async fn capabilities(&self) -> Result<ExternalToolResult> {
        self.request_json("capabilities", json!({})).await
    }

    async fn query_raw_line(&self, line: &str) -> Result<ExternalToolResult> {
        let response = self.with_timeout(send_raw_query(&self.port, line)).await?;
        Ok(ExternalToolResult {
            success: true,
            output: response,
            error: None,
        })
    }

    async fn write_raw_line(&self, line: &str) -> Result<ExternalToolResult> {
        self.with_timeout(send_raw_write(&self.port, line)).await?;
        Ok(ExternalToolResult {
            success: true,
            output: "done".to_string(),
            error: None,
        })
    }

    async fn with_timeout<T>(
        &self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .map_err(|_| anyhow!("serial request timed out after 5s"))?
    }
}

#[cfg(feature = "hardware")]
fn parse_serial_tool_result(response: Value) -> ExternalToolResult {
    let success = response.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let output = response
        .get("result")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| {
            response
                .get("result")
                .cloned()
                .unwrap_or(Value::Null)
                .to_string()
        });
    let error = response
        .get("error")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    ExternalToolResult {
        success,
        output,
        error,
    }
}

#[cfg(feature = "hardware")]
async fn send_json_request(
    port: &Mutex<tokio_serial::SerialStream>,
    command: &str,
    args: Value,
) -> Result<Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    static REQUEST_ID: AtomicU64 = AtomicU64::new(0);
    let request_id = REQUEST_ID.fetch_add(1, Ordering::Relaxed).to_string();
    let request = json!({
        "id": request_id,
        "cmd": command,
        "args": args,
    });
    let line = format!("{}\n", request);

    let mut port = port.lock().await;
    port.write_all(line.as_bytes()).await?;
    port.flush().await?;

    let mut buffer = Vec::new();
    let mut byte = [0u8; 1];
    while port.read_exact(&mut byte).await.is_ok() {
        if byte[0] == b'\n' {
            break;
        }
        buffer.push(byte[0]);
    }

    let response: Value = serde_json::from_slice(&buffer)?;
    let response_id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if response_id != request_id {
        bail!(
            "serial response id mismatch: expected `{}`, got `{}`",
            request_id,
            response_id
        );
    }
    Ok(response)
}

#[cfg(feature = "hardware")]
async fn send_raw_query(port: &Mutex<tokio_serial::SerialStream>, line: &str) -> Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut port = port.lock().await;
    let line = format!("{}\n", line);
    port.write_all(line.as_bytes()).await?;
    port.flush().await?;

    let mut buffer = Vec::new();
    let mut byte = [0u8; 1];
    while port.read_exact(&mut byte).await.is_ok() {
        if byte[0] == b'\n' {
            break;
        }
        buffer.push(byte[0]);
    }
    Ok(String::from_utf8_lossy(&buffer).trim().to_string())
}

#[cfg(feature = "hardware")]
async fn send_raw_write(port: &Mutex<tokio_serial::SerialStream>, line: &str) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut port = port.lock().await;
    let line = format!("{}\n", line);
    port.write_all(line.as_bytes()).await?;
    port.flush().await?;
    Ok(())
}

async fn bridge_request(command: &str, args: &[String]) -> Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let address = "127.0.0.1:9999";
    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(address))
        .await
        .map_err(|_| anyhow!("bridge connection timed out"))??;
    let message = format!("{} {}\n", command, args.join(" "));
    stream.write_all(message.as_bytes()).await?;
    let mut buffer = vec![0u8; 128];
    let read = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut buffer))
        .await
        .map_err(|_| anyhow!("bridge response timed out"))??;
    Ok(String::from_utf8_lossy(&buffer[..read]).trim().to_string())
}

async fn bridge_gpio_read(pin: u64) -> Result<ExternalToolResult> {
    let response = bridge_request("gpio_read", &[pin.to_string()]).await?;
    Ok(bridge_response_to_tool_result(response))
}

async fn bridge_gpio_write(pin: u64, value: u64) -> Result<ExternalToolResult> {
    let response = bridge_request("gpio_write", &[pin.to_string(), value.to_string()]).await?;
    Ok(bridge_response_to_tool_result(response))
}

fn bridge_response_to_tool_result(response: String) -> ExternalToolResult {
    if response.starts_with("error:") {
        ExternalToolResult {
            success: false,
            output: response.clone(),
            error: Some(response),
        }
    } else {
        ExternalToolResult {
            success: true,
            output: response,
            error: None,
        }
    }
}

async fn native_gpio_read(board: &PeripheralBoardConfig, pin: u64) -> Result<ExternalToolResult> {
    if board.board != "rpi-gpio" && board.board != "raspberry-pi" {
        bail!("native gpio is only supported for rpi-gpio boards");
    }

    #[cfg(all(feature = "peripheral-rpi", target_os = "linux"))]
    {
        let pin_u8 = u8::try_from(pin).map_err(|_| anyhow!("pin out of range"))?;
        let value = tokio::task::spawn_blocking(move || {
            let gpio = rppal::gpio::Gpio::new()?;
            let pin = gpio.get(pin_u8)?.into_input();
            Ok::<_, anyhow::Error>(match pin.read() {
                rppal::gpio::Level::Low => 0,
                rppal::gpio::Level::High => 1,
            })
        })
        .await??;
        return Ok(ExternalToolResult {
            success: true,
            output: format!("pin {} = {}", pin, value),
            error: None,
        });
    }

    #[cfg(not(all(feature = "peripheral-rpi", target_os = "linux")))]
    {
        let _ = pin;
        Ok(ExternalToolResult {
            success: false,
            output: String::new(),
            error: Some("native GPIO requires `peripheral-rpi` feature on Linux.".to_string()),
        })
    }
}

async fn native_gpio_write(
    board: &PeripheralBoardConfig,
    pin: u64,
    value: u64,
) -> Result<ExternalToolResult> {
    if board.board != "rpi-gpio" && board.board != "raspberry-pi" {
        bail!("native gpio is only supported for rpi-gpio boards");
    }

    #[cfg(all(feature = "peripheral-rpi", target_os = "linux"))]
    {
        let pin_u8 = u8::try_from(pin).map_err(|_| anyhow!("pin out of range"))?;
        let level = match value {
            0 => rppal::gpio::Level::Low,
            _ => rppal::gpio::Level::High,
        };
        tokio::task::spawn_blocking(move || {
            let gpio = rppal::gpio::Gpio::new()?;
            let mut pin = gpio.get(pin_u8)?.into_output();
            pin.write(level);
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        return Ok(ExternalToolResult {
            success: true,
            output: format!("pin {} = {}", pin, value),
            error: None,
        });
    }

    #[cfg(not(all(feature = "peripheral-rpi", target_os = "linux")))]
    {
        let _ = (pin, value);
        Ok(ExternalToolResult {
            success: false,
            output: String::new(),
            error: Some("native GPIO requires `peripheral-rpi` feature on Linux.".to_string()),
        })
    }
}

struct GpioReadTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl GpioReadTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for GpioReadTool {
    fn name(&self) -> &str {
        "gpio_read"
    }

    fn description(&self) -> &str {
        "Read a GPIO pin from a configured peripheral board."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "transport": { "type": "string" },
                "pin": { "type": "integer" }
            },
            "required": ["pin"]
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_gpio_read(args).await
    }
}

struct GpioWriteTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl GpioWriteTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for GpioWriteTool {
    fn name(&self) -> &str {
        "gpio_write"
    }

    fn description(&self) -> &str {
        "Write a GPIO pin on a configured peripheral board."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "transport": { "type": "string" },
                "pin": { "type": "integer" },
                "value": { "type": "integer" }
            },
            "required": ["pin", "value"]
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_gpio_write(args).await
    }
}

struct SerialQueryTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl SerialQueryTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for SerialQueryTool {
    fn name(&self) -> &str {
        "serial_query"
    }

    fn description(&self) -> &str {
        "Send a serial query to a configured serial peripheral and return its response."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "command": { "type": "string" },
                "args": {},
                "line": { "type": "string" },
                "query": { "type": "string" },
                "payload": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_serial_query(args).await
    }
}

struct SerialWriteTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl SerialWriteTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for SerialWriteTool {
    fn name(&self) -> &str {
        "serial_write"
    }

    fn description(&self) -> &str {
        "Send a serial write command to a configured serial peripheral."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "command": { "type": "string" },
                "args": {},
                "line": { "type": "string" },
                "payload": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_serial_write(args).await
    }
}

struct HardwareCapabilitiesTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl HardwareCapabilitiesTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for HardwareCapabilitiesTool {
    fn name(&self) -> &str {
        "hardware_capabilities"
    }

    fn description(&self) -> &str {
        "Query configured boards for GPIO and other reported capabilities."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_capabilities(args).await
    }
}

struct HardwareBoardInfoTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl HardwareBoardInfoTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for HardwareBoardInfoTool {
    fn name(&self) -> &str {
        "hardware_board_info"
    }

    fn description(&self) -> &str {
        "Return static or probed board info for a configured hardware board."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_board_info(args).await
    }
}

struct HardwareMemoryMapTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl HardwareMemoryMapTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for HardwareMemoryMapTool {
    fn name(&self) -> &str {
        "hardware_memory_map"
    }

    fn description(&self) -> &str {
        "Return the memory map for a board from static data or probe-rs."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_memory_map(args).await
    }
}

struct HardwareMemoryReadTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl HardwareMemoryReadTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for HardwareMemoryReadTool {
    fn name(&self) -> &str {
        "hardware_memory_read"
    }

    fn description(&self) -> &str {
        "Read memory from a supported probe-rs board."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "address": { "type": "string" },
                "length": { "type": "integer" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_memory_read(args).await
    }
}

struct FlashFirmwareTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl FlashFirmwareTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for FlashFirmwareTool {
    fn name(&self) -> &str {
        "flash_firmware"
    }

    fn description(&self) -> &str {
        "Flash firmware to a configured board. Nucleo requires `firmware_path`; Arduino Uno accepts `code`."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "device_selector": { "type": "string" },
                "firmware_path": { "type": "string" },
                "port": { "type": "string" },
                "code": { "type": "string" }
            }
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_flash_firmware(args).await
    }
}

struct ArduinoUploadTool {
    runtime: Arc<PeripheralsRuntime>,
}

impl ArduinoUploadTool {
    fn new(runtime: Arc<PeripheralsRuntime>) -> Self {
        Self { runtime }
    }
}

#[async_trait]
impl ExternalTool for ArduinoUploadTool {
    fn name(&self) -> &str {
        "arduino_upload"
    }

    fn description(&self) -> &str {
        "Compile and upload Arduino Uno code via arduino-cli."
    }

    fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {
                "board": { "type": "string" },
                "port": { "type": "string" },
                "code": { "type": "string" }
            },
            "required": ["code"]
        })
    }

    async fn execute(&self, args: Value) -> Result<ExternalToolResult> {
        self.runtime.execute_arduino_upload(args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn board_aliases_cover_short_forms() {
        let aliases = board_aliases("nucleo-f401re");
        assert!(aliases.contains(&"nucleo".to_string()));
        assert!(aliases.contains(&"nucleo-f401re".to_string()));
    }

    #[test]
    fn device_selector_hint_extracts_board() {
        assert_eq!(
            device_selector_board_hint("board:rpi/main").as_deref(),
            Some("rpi")
        );
        assert_eq!(
            device_selector_board_hint("board:nucleo-f401re"),
            Some("nucleo-f401re".to_string())
        );
    }

    #[tokio::test]
    async fn external_registry_contains_imported_hardware_tools() {
        let registry = build_external_tool_registry(PeripheralsConfig::default())
            .await
            .unwrap();
        assert!(registry.contains("hardware_board_info"));
        assert!(registry.contains("hardware_memory_map"));
        assert!(registry.contains("gpio_write"));
    }

    #[tokio::test]
    async fn board_info_tool_works_without_runtime_config_when_board_explicit() {
        let registry = build_external_tool_registry(PeripheralsConfig::default())
            .await
            .unwrap();
        let tool = registry.get("hardware_board_info").unwrap();
        let result = tool
            .execute(json!({ "board": "nucleo-f401re" }))
            .await
            .unwrap();
        assert!(result.success);
        assert!(result.output.contains("STM32F401RET6"));
    }

    #[tokio::test]
    async fn gpio_write_requires_configured_board() {
        let registry = build_external_tool_registry(PeripheralsConfig::default())
            .await
            .unwrap();
        let tool = registry.get("gpio_write").unwrap();
        let error = tool
            .execute(json!({ "pin": 17, "value": 1 }))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no peripherals configured"));
    }
}
