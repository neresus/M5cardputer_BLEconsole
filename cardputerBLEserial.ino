#include <M5Cardputer.h>
#include <NimBLEDevice.h>
#include <SPI.h>
#include <SD.h>
#include <string>
#include <vector>

namespace {

constexpr const char* SERVICE_UUID = "0000FFE0-0000-1000-8000-00805F9B34FB";
constexpr const char* CHAR_UUID = "0000FFE1-0000-1000-8000-00805F9B34FB";

constexpr const char* DEVICES_FILE = "/ble_devices.txt";
constexpr const char* COMMANDS_FILE = "/ble_commands.txt";

constexpr int SD_SPI_SCK_PIN = 40;
constexpr int SD_SPI_MISO_PIN = 39;
constexpr int SD_SPI_MOSI_PIN = 14;
constexpr int SD_SPI_CS_PIN = 12;

constexpr int SCREEN_WIDTH = 240;
constexpr int SCREEN_HEIGHT = 135;
constexpr int HEADER_HEIGHT = 14;
constexpr int FOOTER_HEIGHT = 11;
constexpr int INPUT_HEIGHT = 18;
constexpr int PANEL_WIDTH = 90;
constexpr int LINE_HEIGHT = 10;
constexpr int MAX_LOG_LINES = 50;
constexpr uint32_t SCAN_DURATION_MS = 6000;
constexpr int TERMINAL_VISIBLE_ROWS = 7;

enum AppState {
  STATE_BOOT,
  STATE_SCANNING,
  STATE_SELECT_DEVICE,
  STATE_CONNECTING,
  STATE_CONNECTED,
  STATE_DISCONNECTED,
  STATE_ERROR
};

enum FocusArea {
  FOCUS_INPUT,
  FOCUS_COMMANDS,
  FOCUS_LOGS
};

struct PreferredDevice {
  String address;
  String label;
  String password;
  String pattern;
};

struct PredefinedCommand {
  String label;
  String command;
};

struct DiscoveredDevice {
  String name;
  String address;
  uint8_t addressType = 0;
  int rssi = 0;
  bool preferred = false;
  String preferredLabel;
};

AppState appState = STATE_BOOT;
FocusArea focusArea = FOCUS_INPUT;

std::vector<PreferredDevice> preferredDevices;
std::vector<PredefinedCommand> predefinedCommands;
std::vector<DiscoveredDevice> selectableDevices;
std::vector<String> logLines;

String statusLine = "Booting";
String statusDetail = "";
String inputBuffer = "";
String connectedName = "";
String connectedAddress = "";
String connectedPassword = "";
String connectedPattern = "";
String connectedCaptureValue = "";
String rxPartialLine = "";

bool sdReady = false;
bool preferredFileLoaded = false;
bool commandsFileLoaded = false;
bool showingFallbackDevices = false;
bool uiDirty = true;

int selectedDeviceIndex = 0;
int selectedCommandIndex = 0;
int commandScrollOffset = 0;
int logScrollOffset = 0;

volatile bool disconnectPending = false;
volatile int disconnectReason = 0;
volatile bool scanCompletePending = false;
volatile int scanEndReason = 0;
bool scanInProgress = false;

NimBLEClient* bleClient = nullptr;
NimBLERemoteCharacteristic* remoteCharacteristic = nullptr;
NimBLEScan* bleScan = nullptr;
std::vector<DiscoveredDevice> scannedFfe0Devices;
std::vector<DiscoveredDevice> scannedFallbackDevices;

class CardputerBleClientCallbacks : public NimBLEClientCallbacks {
 public:
  void onDisconnect(NimBLEClient* client, int reason) override {
    (void)client;
    disconnectReason = reason;
    disconnectPending = true;
  }
};

CardputerBleClientCallbacks bleClientCallbacks;

class CardputerScanCallbacks : public NimBLEScanCallbacks {
 public:
  void onResult(const NimBLEAdvertisedDevice* advertisedDevice) override;
  void onScanEnd(const NimBLEScanResults& results, int reason) override;
};

CardputerScanCallbacks bleScanCallbacks;

void processCapturePattern(const String& text);

String trimCopy(String value) {
  value.trim();
  return value;
}

String normalizeAddress(String value) {
  value = trimCopy(value);
  value.toUpperCase();
  return value;
}

String shortText(const String& text, int maxChars) {
  if (maxChars <= 0 || text.length() <= maxChars) {
    return text;
  }

  if (maxChars <= 3) {
    return text.substring(0, maxChars);
  }

  return text.substring(0, maxChars - 3) + "...";
}

void markUiDirty() {
  uiDirty = true;
}

void addLogLine(String text) {
  text.replace("\r", "");
  text = trimCopy(text);
  if (text.length() == 0) {
    return;
  }

  if (logLines.size() >= MAX_LOG_LINES) {
    logLines.erase(logLines.begin());
  }
  logLines.push_back(text);
  if (focusArea != FOCUS_LOGS) {
    logScrollOffset = 0;
  }
  processCapturePattern(text);
  Serial.println(text);
  markUiDirty();
}

void processCapturePattern(const String& text) {
  if (connectedPattern.length() == 0) {
    return;
  }

  const int matchIndex = text.indexOf(connectedPattern);
  if (matchIndex < 0) {
    return;
  }

  connectedCaptureValue = trimCopy(text.substring(matchIndex));
  markUiDirty();
}

void setStatus(const String& headline, const String& detail = "") {
  statusLine = headline;
  statusDetail = detail;
  markUiDirty();
}

int findPreferredIndexByAddress(const String& address) {
  const String normalized = normalizeAddress(address);
  for (size_t i = 0; i < preferredDevices.size(); ++i) {
    if (preferredDevices[i].address == normalized) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

bool sdBegin() {
  SPI.begin(SD_SPI_SCK_PIN, SD_SPI_MISO_PIN, SD_SPI_MOSI_PIN, SD_SPI_CS_PIN);
  if (!SD.begin(SD_SPI_CS_PIN, SPI, 25000000)) {
    sdReady = false;
    addLogLine("SD unavailable");
    return false;
  }

  sdReady = true;
  addLogLine("SD ready");
  return true;
}

bool parseDeviceLine(const String& rawLine, PreferredDevice& outDevice) {
  String line = trimCopy(rawLine);
  if (line.length() == 0 || line.startsWith("#")) {
    return false;
  }

  int firstSep = line.indexOf('|');
  int secondSep = firstSep >= 0 ? line.indexOf('|', firstSep + 1) : -1;
  int thirdSep = secondSep >= 0 ? line.indexOf('|', secondSep + 1) : -1;
  String address = firstSep >= 0 ? line.substring(0, firstSep) : line;
  String label = firstSep >= 0
    ? (secondSep >= 0 ? line.substring(firstSep + 1, secondSep) : line.substring(firstSep + 1))
    : "";
  String password = secondSep >= 0
    ? (thirdSep >= 0 ? line.substring(secondSep + 1, thirdSep) : line.substring(secondSep + 1))
    : "";
  String pattern = thirdSep >= 0 ? line.substring(thirdSep + 1) : "";

  address = normalizeAddress(address);
  label = trimCopy(label);
  password = trimCopy(password);
  pattern = trimCopy(pattern);

  if (address.length() < 11 || address.indexOf(':') < 0) {
    return false;
  }

  outDevice.address = address;
  outDevice.label = label;
  outDevice.password = password;
  outDevice.pattern = pattern;
  return true;
}

bool savePreferredDevices() {
  if (!sdReady) {
    return false;
  }

  SD.remove(DEVICES_FILE);
  File file = SD.open(DEVICES_FILE, FILE_WRITE);
  if (!file) {
    return false;
  }

  file.println("# Preferred devices by BLE address.");
  file.println("# Format:");
  file.println("# AA:BB:CC:DD:EE:FF|Friendly label|pass|pattern");
  file.println();

  for (size_t i = 0; i < preferredDevices.size(); ++i) {
    const PreferredDevice& device = preferredDevices[i];
    file.print(device.address);
    file.print("|");
    file.print(device.label);
    file.print("|");
    file.print(device.password);
    file.print("|");
    file.println(device.pattern);
  }

  file.close();
  return true;
}

bool parseCommandLine(const String& rawLine, PredefinedCommand& outCommand) {
  String line = trimCopy(rawLine);
  if (line.length() == 0 || line.startsWith("#")) {
    return false;
  }

  int sepIndex = line.indexOf('|');
  String label = sepIndex >= 0 ? line.substring(0, sepIndex) : "";
  String command = sepIndex >= 0 ? line.substring(sepIndex + 1) : line;

  label = trimCopy(label);
  command = trimCopy(command);

  if (command.length() == 0) {
    return false;
  }

  outCommand.command = command;
  outCommand.label = label.length() == 0 ? command : label;
  return true;
}

template <typename T>
bool loadConfigList(const char* path, std::vector<T>& destination, bool (*parser)(const String&, T&)) {
  destination.clear();

  if (!sdReady) {
    return false;
  }

  File file = SD.open(path, FILE_READ);
  if (!file) {
    return false;
  }

  while (file.available()) {
    String line = file.readStringUntil('\n');
    T item;
    if (parser(line, item)) {
      destination.push_back(item);
    }
  }

  file.close();
  return true;
}

void loadSdConfig() {
  preferredFileLoaded = loadConfigList<PreferredDevice>(DEVICES_FILE, preferredDevices, parseDeviceLine);
  commandsFileLoaded = loadConfigList<PredefinedCommand>(COMMANDS_FILE, predefinedCommands, parseCommandLine);
  selectedCommandIndex = 0;

  addLogLine(preferredFileLoaded ? "Loaded preferred devices" : "No preferred device file");
  addLogLine(commandsFileLoaded ? "Loaded predefined commands" : "No commands file");
}

String deviceDisplayName(const DiscoveredDevice& device) {
  if (device.preferred && device.preferredLabel.length() > 0) {
    return device.preferredLabel;
  }
  if (device.name.length() > 0) {
    return device.name;
  }
  return device.address;
}

PreferredDevice* getConnectedPreferredDevice() {
  if (connectedAddress.length() == 0) {
    return nullptr;
  }

  const int index = findPreferredIndexByAddress(connectedAddress);
  if (index < 0) {
    return nullptr;
  }

  return &preferredDevices[index];
}

int getVisibleCommandCount() {
  return static_cast<int>(predefinedCommands.size()) + (connectedPassword.length() > 0 ? 1 : 0);
}

String getCommandLabel(int index) {
  if (connectedPassword.length() > 0) {
    if (index == 0) {
      return "Password: " + connectedPassword;
    }
    index -= 1;
  }

  return predefinedCommands[index].label;
}

String getCommandValue(int index) {
  if (connectedPassword.length() > 0) {
    if (index == 0) {
      return connectedPassword;
    }
    index -= 1;
  }

  return predefinedCommands[index].command;
}

void updateConnectedPassword(const String& newPassword) {
  connectedPassword = trimCopy(newPassword);

  PreferredDevice* preferred = getConnectedPreferredDevice();
  if (preferred) {
    preferred->password = connectedPassword;
  } else if (connectedAddress.length() > 0) {
    PreferredDevice device;
    device.address = connectedAddress;
    device.label = connectedName;
    device.password = connectedPassword;
    device.pattern = connectedPattern;
    preferredDevices.push_back(device);
  }

  if (sdReady) {
    if (savePreferredDevices()) {
      addLogLine("Saved password to SD");
    } else {
      addLogLine("Failed to save password");
    }
  }

  selectedCommandIndex = 0;
  commandScrollOffset = 0;
  markUiDirty();
}

bool hasHidKey(const Keyboard_Class::KeysState& keys, uint8_t keyCode) {
  for (uint8_t code : keys.hid_keys) {
    if (code == keyCode) {
      return true;
    }
  }
  return false;
}

bool hasWordKey(const Keyboard_Class::KeysState& keys, char c) {
  for (char value : keys.word) {
    if (value == c) {
      return true;
    }
  }
  return false;
}

bool containsDevice(
  const std::vector<DiscoveredDevice>& devices,
  const String& address,
  const String& name,
  int rssi
) {
  (void)name;
  (void)rssi;
  for (size_t i = 0; i < devices.size(); ++i) {
    const DiscoveredDevice& device = devices[i];
    if (device.address == address) {
      return true;
    }
  }
  return false;
}

bool isArrowUp(const Keyboard_Class::KeysState& keys) {
  return hasHidKey(keys, 0x52) || (keys.fn && (hasWordKey(keys, 'k') || hasWordKey(keys, 'K')));
}

bool isArrowDown(const Keyboard_Class::KeysState& keys) {
  return hasHidKey(keys, 0x51) || (keys.fn && (hasWordKey(keys, 'j') || hasWordKey(keys, 'J')));
}

void releaseBleClient(bool disconnectFirst) {
  remoteCharacteristic = nullptr;

  if (!bleClient) {
    return;
  }

  if (disconnectFirst && bleClient->isConnected()) {
    bleClient->disconnect();
  }

  NimBLEDevice::deleteClient(bleClient);
  bleClient = nullptr;
}

void handleIncomingPayload(const uint8_t* data, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    const char c = static_cast<char>(data[i]);
    if (c == '\r') {
      continue;
    }

    if (c == '\n') {
      if (rxPartialLine.length() > 0) {
        addLogLine(rxPartialLine);
        rxPartialLine = "";
      }
      continue;
    }

    rxPartialLine += c;
    if (rxPartialLine.length() > 96) {
      addLogLine(rxPartialLine);
      rxPartialLine = "";
    }
  }

  if (rxPartialLine.length() > 0 && len > 0 && data[len - 1] != '\n') {
    markUiDirty();
  }
}

void notifyCallback(NimBLERemoteCharacteristic* characteristic, uint8_t* data, size_t len, bool isNotify) {
  (void)characteristic;
  (void)isNotify;
  handleIncomingPayload(data, len);
}

void drawHeader(const String& title, uint16_t color) {
  M5Cardputer.Display.fillRect(0, 0, SCREEN_WIDTH, HEADER_HEIGHT, color);
  M5Cardputer.Display.setTextColor(BLACK, color);
  M5Cardputer.Display.setCursor(3, 3);
  M5Cardputer.Display.print(shortText(title, 36));
}

void drawFooter(const String& text) {
  const int y = SCREEN_HEIGHT - FOOTER_HEIGHT;
  M5Cardputer.Display.fillRect(0, y, SCREEN_WIDTH, FOOTER_HEIGHT, DARKGREY);
  M5Cardputer.Display.setTextColor(WHITE, DARKGREY);
  M5Cardputer.Display.setCursor(2, y + 2);
  M5Cardputer.Display.print(shortText(text, 42));
}

void drawCenteredText(const String& line1, const String& line2 = "") {
  M5Cardputer.Display.setTextColor(WHITE, BLACK);
  M5Cardputer.Display.setCursor(14, 42);
  M5Cardputer.Display.print(shortText(line1, 34));
  if (line2.length() > 0) {
    M5Cardputer.Display.setCursor(14, 62);
    M5Cardputer.Display.print(shortText(line2, 34));
  }
}

void renderBootScreen() {
  drawHeader("BLE Serial", GREEN);
  drawCenteredText(statusLine, statusDetail);
  drawFooter("Preparing app");
}

void renderScanningScreen() {
  drawHeader("Scanning", YELLOW);
  drawCenteredText(statusLine, statusDetail);
  drawFooter("Please wait");
}

void renderSelectionScreen() {
  String title = "BLE UART Devices";
  if (showingFallbackDevices) {
    title = "All BLE Devices";
  }

  drawHeader(title, CYAN);

  const int listY = HEADER_HEIGHT + 4;
  const int listHeight = SCREEN_HEIGHT - HEADER_HEIGHT - FOOTER_HEIGHT - 4;
  const int visibleRows = listHeight / LINE_HEIGHT;
  int topIndex = 0;
  if (selectedDeviceIndex >= visibleRows) {
    topIndex = selectedDeviceIndex - visibleRows + 1;
  }

  M5Cardputer.Display.drawRect(0, HEADER_HEIGHT, SCREEN_WIDTH, SCREEN_HEIGHT - HEADER_HEIGHT - FOOTER_HEIGHT, WHITE);
  for (int row = 0; row < visibleRows; ++row) {
    const int deviceIndex = topIndex + row;
    if (deviceIndex >= static_cast<int>(selectableDevices.size())) {
      break;
    }

    const int y = listY + row * LINE_HEIGHT;
    const bool selected = deviceIndex == selectedDeviceIndex;
    const uint16_t bg = selected ? BLUE : BLACK;
    const uint16_t fg = selected ? WHITE : GREEN;

    M5Cardputer.Display.fillRect(2, y - 1, SCREEN_WIDTH - 4, LINE_HEIGHT, bg);
    M5Cardputer.Display.setTextColor(fg, bg);

    const DiscoveredDevice& device = selectableDevices[deviceIndex];
    String rowText = String(device.preferred ? "* " : "  ")
      + deviceDisplayName(device)
      + " "
      + shortText(device.address, 17)
      + " ["
      + String(device.rssi)
      + "]";
    M5Cardputer.Display.setCursor(4, y);
    M5Cardputer.Display.print(shortText(rowText, 34));
  }

  drawFooter("Up/Down select Enter connect");
}

void drawLogArea(int x, int y, int width, int height) {
  M5Cardputer.Display.drawRect(x, y, width, height, focusArea == FOCUS_LOGS ? YELLOW : WHITE);

  const int visibleRows = TERMINAL_VISIBLE_ROWS;
  const int maxStart = logLines.size() > static_cast<size_t>(visibleRows)
                         ? static_cast<int>(logLines.size()) - visibleRows
                         : 0;
  if (logScrollOffset > maxStart) {
    logScrollOffset = maxStart;
  }
  const int start = maxStart - logScrollOffset;
  const int maxChars = (width / 6) - 1;

  int drawY = y + 2;
  M5Cardputer.Display.setTextColor(WHITE, BLACK);
  for (size_t i = start; i < logLines.size(); ++i) {
    M5Cardputer.Display.setCursor(x + 3, drawY);
    M5Cardputer.Display.print(shortText(logLines[i], maxChars));
    drawY += LINE_HEIGHT;
  }

  if (rxPartialLine.length() > 0 && drawY < y + height - 2) {
    M5Cardputer.Display.setTextColor(YELLOW, BLACK);
    M5Cardputer.Display.setCursor(x + 3, drawY);
    M5Cardputer.Display.print(shortText(rxPartialLine + "_", maxChars));
  }
}

void drawCommandsPanel(int x, int y, int width, int height) {
  M5Cardputer.Display.drawRect(x, y, width, height, focusArea == FOCUS_COMMANDS ? YELLOW : DARKGREY);
  M5Cardputer.Display.setTextColor(CYAN, BLACK);
  M5Cardputer.Display.setCursor(x + 3, y + 2);
  M5Cardputer.Display.print("Commands");

  const int totalCommands = getVisibleCommandCount();
  if (totalCommands == 0) {
    M5Cardputer.Display.setTextColor(DARKGREY, BLACK);
    M5Cardputer.Display.setCursor(x + 3, y + 16);
    M5Cardputer.Display.print("No commands");
    return;
  }

  const int listY = y + 14;
  const int visibleRows = TERMINAL_VISIBLE_ROWS;
  const int maxTop = totalCommands > visibleRows
                       ? totalCommands - visibleRows
                       : 0;
  if (commandScrollOffset > maxTop) {
    commandScrollOffset = maxTop;
  }
  int topIndex = commandScrollOffset;
  if (selectedCommandIndex < topIndex) {
    topIndex = selectedCommandIndex;
  }
  if (selectedCommandIndex >= topIndex + visibleRows) {
    topIndex = selectedCommandIndex - visibleRows + 1;
  }
  commandScrollOffset = topIndex;

  for (int row = 0; row < visibleRows; ++row) {
    const int index = topIndex + row;
    if (index >= totalCommands) {
      break;
    }

    const bool selected = index == selectedCommandIndex;
    const int drawY = listY + row * LINE_HEIGHT;
    const uint16_t bg = selected ? BLUE : BLACK;
    const uint16_t fg = selected ? WHITE : GREEN;

    M5Cardputer.Display.fillRect(x + 2, drawY - 1, width - 4, LINE_HEIGHT, bg);
    M5Cardputer.Display.setTextColor(fg, bg);
    M5Cardputer.Display.setCursor(x + 4, drawY);
    M5Cardputer.Display.print(shortText(getCommandLabel(index), (width / 6) - 2));
  }
}

void renderTerminalScreen() {
  drawHeader("Connected", GREEN);
  M5Cardputer.Display.setTextColor(WHITE, BLACK);
  M5Cardputer.Display.setCursor(4, HEADER_HEIGHT + 2);
  M5Cardputer.Display.print(shortText(connectedName.length() > 0 ? connectedName : connectedAddress, 18));
  M5Cardputer.Display.setTextColor(CYAN, BLACK);
  M5Cardputer.Display.setCursor(118, HEADER_HEIGHT + 2);
  M5Cardputer.Display.print(shortText(connectedCaptureValue, 19));

  const bool showCommands = true;
  const int mainY = HEADER_HEIGHT + 12;
  const int mainHeight = SCREEN_HEIGHT - HEADER_HEIGHT - INPUT_HEIGHT - FOOTER_HEIGHT - 12;
  const int commandsWidth = showCommands ? PANEL_WIDTH : 0;
  const int logWidth = SCREEN_WIDTH - commandsWidth - (showCommands ? 2 : 0);

  drawLogArea(0, mainY, logWidth, mainHeight);

  if (showCommands) {
    drawCommandsPanel(logWidth + 2, mainY, SCREEN_WIDTH - logWidth - 2, mainHeight);
  }

  const int inputY = SCREEN_HEIGHT - FOOTER_HEIGHT - INPUT_HEIGHT;
  const uint16_t inputBorder = focusArea == FOCUS_INPUT ? YELLOW : DARKGREY;
  M5Cardputer.Display.drawRect(0, inputY, SCREEN_WIDTH, INPUT_HEIGHT, inputBorder);
  M5Cardputer.Display.setTextColor(WHITE, BLACK);
  M5Cardputer.Display.setCursor(3, inputY + 4);
  M5Cardputer.Display.print("> ");

  const int maxChars = (SCREEN_WIDTH / 6) - 5;
  String visibleInput = inputBuffer;
  if (visibleInput.length() > maxChars) {
    visibleInput = visibleInput.substring(visibleInput.length() - maxChars);
  }
  M5Cardputer.Display.print(visibleInput);

  String footerText = focusArea == FOCUS_COMMANDS
                        ? "Tab logs ; up . down Enter load"
                        : (focusArea == FOCUS_LOGS
                            ? "Tab input ; up . down BtnA rescan"
                            : "Tab commands Enter send BtnA rescan");
  drawFooter(footerText);
}

void renderDisconnectedScreen(uint16_t color, const String& footerText) {
  drawHeader(statusLine, color);
  drawCenteredText(statusDetail, connectedAddress);
  drawFooter(footerText);
}

void renderUi() {
  if (!uiDirty) {
    return;
  }

  M5Cardputer.Display.fillScreen(BLACK);
  M5Cardputer.Display.setTextSize(1);
  M5Cardputer.Display.setTextWrap(false);

  switch (appState) {
    case STATE_BOOT:
      renderBootScreen();
      break;
    case STATE_SCANNING:
      renderScanningScreen();
      break;
    case STATE_SELECT_DEVICE:
      renderSelectionScreen();
      break;
    case STATE_CONNECTING:
      drawHeader("Connecting", ORANGE);
      drawCenteredText(statusLine, statusDetail);
      drawFooter("Please wait");
      break;
    case STATE_CONNECTED:
      renderTerminalScreen();
      break;
    case STATE_DISCONNECTED:
      renderDisconnectedScreen(RED, "Enter or BtnA to rescan");
      break;
    case STATE_ERROR:
      renderDisconnectedScreen(MAGENTA, "BtnA to retry");
      break;
  }

  uiDirty = false;
}

void enterDisconnectedState(const String& headline, const String& detail) {
  setStatus(headline, detail);
  appState = STATE_DISCONNECTED;
  focusArea = FOCUS_INPUT;
  connectedName = "";
  connectedAddress = "";
  connectedPassword = "";
  connectedPattern = "";
  connectedCaptureValue = "";
  commandScrollOffset = 0;
  logScrollOffset = 0;
}

void processDisconnectEvent() {
  if (!disconnectPending) {
    return;
  }

  disconnectPending = false;
  const int reason = disconnectReason;
  releaseBleClient(false);
  addLogLine("Disconnected");
  enterDisconnectedState("Disconnected", "Reason " + String(reason));
}

void finalizeScanResults() {
  int preferredFfe0Count = 0;
  for (size_t i = 0; i < scannedFfe0Devices.size(); ++i) {
    if (scannedFfe0Devices[i].preferred) {
      preferredFfe0Count++;
    }
  }

  selectableDevices = scannedFfe0Devices;
  if (!selectableDevices.empty()) {
    showingFallbackDevices = false;
    selectedDeviceIndex = 0;
    setStatus(
      preferredFfe0Count > 0 ? "FFE0 devices found" : "Scan complete",
      String(selectableDevices.size()) + " UART device(s)"
    );
    appState = STATE_SELECT_DEVICE;
    return;
  }

  selectableDevices = scannedFallbackDevices;
  showingFallbackDevices = !selectableDevices.empty();
  selectedDeviceIndex = 0;
  if (!selectableDevices.empty()) {
    setStatus("UART not advertised", String(selectableDevices.size()) + " BLE device(s)");
    appState = STATE_SELECT_DEVICE;
    return;
  }

  addLogLine("No BLE devices found");
  enterDisconnectedState("No devices found", "Rescan to try again");
}

void startScan() {
  releaseBleClient(true);
  disconnectPending = false;
  selectableDevices.clear();
  scannedFfe0Devices.clear();
  scannedFallbackDevices.clear();
  selectedDeviceIndex = 0;
  showingFallbackDevices = false;
  focusArea = FOCUS_INPUT;
  commandScrollOffset = 0;
  logScrollOffset = 0;
  rxPartialLine = "";
  addLogLine("Starting BLE scan");

  appState = STATE_SCANNING;
  setStatus("Scanning BLE", "Looking for UART devices");
  renderUi();

  scanCompletePending = false;
  scanEndReason = 0;
  scanInProgress = true;

  bleScan->stop();
  bleScan->clearResults();
  bleScan->setActiveScan(true);
  bleScan->setInterval(45);
  bleScan->setWindow(15);
  bleScan->setMaxResults(0);
  bleScan->setDuplicateFilter(0);
  bleScan->setScanCallbacks(&bleScanCallbacks, true);

  setStatus("Scanning BLE", "Waiting 6 seconds");
  renderUi();

  if (!bleScan->start(SCAN_DURATION_MS, false, true)) {
    scanInProgress = false;
    appState = STATE_ERROR;
    setStatus("Scan failed", "BLE scan did not start");
    addLogLine("Scan failed");
  }
}

bool sendSerialLine(const String& payload) {
  if (!remoteCharacteristic || !bleClient || !bleClient->isConnected()) {
    addLogLine("Not connected");
    return false;
  }

  const String outgoing = payload + "\r\n";
  bool ok = false;
  if (remoteCharacteristic->canWrite()) {
    ok = remoteCharacteristic->writeValue(reinterpret_cast<const uint8_t*>(outgoing.c_str()), outgoing.length(), true);
  } else if (remoteCharacteristic->canWriteNoResponse()) {
    ok = remoteCharacteristic->writeValue(reinterpret_cast<const uint8_t*>(outgoing.c_str()), outgoing.length(), false);
  }

  if (ok) {
    addLogLine("> " + payload);
  } else {
    addLogLine("Send failed");
  }
  return ok;
}

bool connectSelectedDevice() {
  if (selectedDeviceIndex < 0 || selectedDeviceIndex >= static_cast<int>(selectableDevices.size())) {
    return false;
  }

  const DiscoveredDevice selected = selectableDevices[selectedDeviceIndex];
  connectedName = deviceDisplayName(selected);
  connectedAddress = selected.address;
  connectedPassword = "";
  connectedPattern = "";
  connectedCaptureValue = "";
  const int preferredIndex = findPreferredIndexByAddress(selected.address);
  if (preferredIndex >= 0) {
    connectedPassword = preferredDevices[preferredIndex].password;
    connectedPattern = preferredDevices[preferredIndex].pattern;
  }
  rxPartialLine = "";

  appState = STATE_CONNECTING;
  setStatus("Connecting", connectedName);
  renderUi();

  releaseBleClient(true);
  disconnectPending = false;

  bleClient = NimBLEDevice::createClient();
  if (!bleClient) {
    appState = STATE_ERROR;
    setStatus("Client error", "Unable to create BLE client");
    return false;
  }

  bleClient->setClientCallbacks(&bleClientCallbacks, false);
  bleClient->setConnectTimeout(5000);

  NimBLEAddress peerAddress(std::string(selected.address.c_str()), selected.addressType);
  if (!bleClient->connect(peerAddress)) {
    addLogLine("Connect failed: " + selected.address);
    releaseBleClient(false);
    enterDisconnectedState("Connect failed", selected.address);
    return false;
  }

  NimBLERemoteService* service = bleClient->getService(SERVICE_UUID);
  if (!service) {
    addLogLine("Missing FFE0 service");
    releaseBleClient(true);
    enterDisconnectedState("Service not found", "FFE0 missing");
    return false;
  }

  remoteCharacteristic = service->getCharacteristic(CHAR_UUID);
  if (!remoteCharacteristic) {
    addLogLine("Missing FFE1 characteristic");
    releaseBleClient(true);
    enterDisconnectedState("Characteristic missing", "FFE1 missing");
    return false;
  }

  if (remoteCharacteristic->canNotify() || remoteCharacteristic->canIndicate()) {
    if (!remoteCharacteristic->subscribe(remoteCharacteristic->canNotify(), notifyCallback, true)) {
      addLogLine("Notify subscribe failed");
    }
  }

  appState = STATE_CONNECTED;
  focusArea = FOCUS_INPUT;
  addLogLine("BLE connected: " + connectedName);
  setStatus("Connected", connectedName);
  return true;
}

void moveSelection(int direction, int& index, int itemCount) {
  if (itemCount <= 0) {
    index = 0;
    return;
  }

  index += direction;
  if (index < 0) {
    index = 0;
  } else if (index >= itemCount) {
    index = itemCount - 1;
  }
  markUiDirty();
}

void moveScroll(int direction, int& offset, int itemCount, int visibleRows) {
  const int maxOffset = itemCount > visibleRows ? itemCount - visibleRows : 0;
  offset += direction;
  if (offset < 0) {
    offset = 0;
  } else if (offset > maxOffset) {
    offset = maxOffset;
  }
  markUiDirty();
}

void appendTypedCharacters(const Keyboard_Class::KeysState& keys) {
  for (char c : keys.word) {
    if (keys.fn && (c == 'j' || c == 'J' || c == 'k' || c == 'K')) {
      continue;
    }
    inputBuffer += c;
  }

  if (keys.space && !hasWordKey(keys, ' ')) {
    inputBuffer += ' ';
  }
}

void handleDeviceSelectionKeys(const Keyboard_Class::KeysState& keys) {
  if (isArrowUp(keys) || hasWordKey(keys, ';')) {
    moveSelection(-1, selectedDeviceIndex, static_cast<int>(selectableDevices.size()));
    return;
  }

  if (isArrowDown(keys) || hasWordKey(keys, '.')) {
    moveSelection(1, selectedDeviceIndex, static_cast<int>(selectableDevices.size()));
    return;
  }

  if (keys.enter) {
    connectSelectedDevice();
  }
}

void handleConnectedKeys(const Keyboard_Class::KeysState& keys) {
  if (keys.tab) {
    if (focusArea == FOCUS_INPUT) {
      focusArea = FOCUS_COMMANDS;
    } else if (focusArea == FOCUS_COMMANDS) {
      focusArea = FOCUS_LOGS;
    } else {
      focusArea = FOCUS_INPUT;
    }
    markUiDirty();
    return;
  }

  if (focusArea == FOCUS_COMMANDS) {
    if (isArrowUp(keys) || hasWordKey(keys, ';')) {
      moveSelection(-1, selectedCommandIndex, getVisibleCommandCount());
      moveScroll(-1, commandScrollOffset, getVisibleCommandCount(), TERMINAL_VISIBLE_ROWS);
      return;
    }

    if (isArrowDown(keys) || hasWordKey(keys, '.')) {
      moveSelection(1, selectedCommandIndex, getVisibleCommandCount());
      moveScroll(1, commandScrollOffset, getVisibleCommandCount(), TERMINAL_VISIBLE_ROWS);
      return;
    }

    if (keys.enter && getVisibleCommandCount() > 0) {
      inputBuffer = getCommandValue(selectedCommandIndex);
      focusArea = FOCUS_INPUT;
      markUiDirty();
    }
    return;
  }

  if (focusArea == FOCUS_LOGS) {
    const int visibleRows = TERMINAL_VISIBLE_ROWS;
    if (isArrowUp(keys) || hasWordKey(keys, ';')) {
      moveScroll(1, logScrollOffset, static_cast<int>(logLines.size()), visibleRows);
      return;
    }

    if (isArrowDown(keys) || hasWordKey(keys, '.')) {
      moveScroll(-1, logScrollOffset, static_cast<int>(logLines.size()), visibleRows);
      return;
    }
    return;
  }

  appendTypedCharacters(keys);

  if (keys.del && inputBuffer.length() > 0) {
    inputBuffer.remove(inputBuffer.length() - 1);
  }

  if (keys.enter && inputBuffer.length() > 0) {
    if (inputBuffer.startsWith("pass=")) {
      updateConnectedPassword(inputBuffer.substring(5));
      addLogLine("Password updated");
    } else {
      sendSerialLine(inputBuffer);
    }
    inputBuffer = "";
  }

  markUiDirty();
}

void handleGlobalActions() {
  if (M5Cardputer.BtnA.wasPressed()) {
    if (appState == STATE_CONNECTED) {
      releaseBleClient(true);
      disconnectPending = false;
      enterDisconnectedState("Reconnect ready", "Press Enter or BtnA");
    } else {
      startScan();
    }
  }
}

void processScanEndEvent() {
  if (!scanCompletePending) {
    return;
  }

  scanCompletePending = false;
  scanInProgress = false;
  addLogLine("Scan ended reason " + String(scanEndReason));
  addLogLine("Scan saw " + String(scannedFfe0Devices.size() + scannedFallbackDevices.size()) + " cached device(s)");
  finalizeScanResults();
}

void updateKeyboardInput() {
  if (!M5Cardputer.Keyboard.isChange() || !M5Cardputer.Keyboard.isPressed()) {
    return;
  }

  Keyboard_Class::KeysState keys = M5Cardputer.Keyboard.keysState();

  switch (appState) {
    case STATE_SELECT_DEVICE:
      handleDeviceSelectionKeys(keys);
      break;
    case STATE_CONNECTED:
      handleConnectedKeys(keys);
      break;
    case STATE_DISCONNECTED:
    case STATE_ERROR:
      if (keys.enter && !scanInProgress) {
        startScan();
      }
      break;
    default:
      break;
  }
}

}  // namespace

void setup() {
  Serial.begin(115200);

  auto cfg = M5.config();
  M5Cardputer.begin(cfg, true);
  M5Cardputer.Display.setRotation(1);
  M5Cardputer.Display.setTextSize(1);
  M5Cardputer.Display.setTextColor(WHITE, BLACK);

  addLogLine("BLE Serial starting");

  NimBLEDevice::init("");
  bleScan = NimBLEDevice::getScan();

  sdBegin();
  loadSdConfig();

  appState = STATE_BOOT;
  setStatus("BLE Serial", "Loading configuration");
  renderUi();

  startScan();
}

void loop() {
  M5Cardputer.update();

  processDisconnectEvent();
  processScanEndEvent();
  handleGlobalActions();
  updateKeyboardInput();
  renderUi();
}

void CardputerScanCallbacks::onResult(const NimBLEAdvertisedDevice* advertisedDevice) {
  if (!advertisedDevice) {
    return;
  }

  const String address = normalizeAddress(String(advertisedDevice->getAddress().toString().c_str()));
  const String name = String(advertisedDevice->getName().c_str());
  const int rssi = advertisedDevice->getRSSI();
  const int preferredIndex = findPreferredIndexByAddress(address);
  const bool preferred = preferredIndex >= 0;

  DiscoveredDevice scanned;
  scanned.address = address;
  scanned.name = name;
  scanned.addressType = advertisedDevice->getAddressType();
  scanned.rssi = rssi;
  scanned.preferred = preferred;
  if (preferred) {
    scanned.preferredLabel = preferredDevices[preferredIndex].label;
  }

  if (advertisedDevice->isAdvertisingService(NimBLEUUID(SERVICE_UUID))) {
    if (!containsDevice(scannedFfe0Devices, address, name, rssi)) {
      scannedFfe0Devices.push_back(scanned);
      String debugName = scanned.name.length() > 0 ? scanned.name : "(no name)";
      addLogLine(shortText("FFE0 " + debugName + " " + scanned.address, 42));
    }
  } else if (advertisedDevice->isConnectable()) {
    if (!containsDevice(scannedFallbackDevices, address, name, rssi)) {
      scannedFallbackDevices.push_back(scanned);
    }
  }
}

void CardputerScanCallbacks::onScanEnd(const NimBLEScanResults& results, int reason) {
  (void)results;
  scanEndReason = reason;
  scanCompletePending = true;
}
