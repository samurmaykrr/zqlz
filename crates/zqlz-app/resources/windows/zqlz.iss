; ZQLZ Inno Setup Installer Script
; This script creates a Windows installer for ZQLZ

[Setup]
AppId={#AppId}
AppName={#AppName}
AppVerName={#AppDisplayName}
AppPublisher=ZQLZ
AppPublisherURL=https://zqlz.dev/
AppSupportURL=https://zqlz.dev/support
AppUpdatesURL=https://zqlz.dev/releases
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
DisableReadyPage=yes
AllowNoIcons=yes
OutputDir={#OutputDir}
OutputBaseFilename={#AppSetupName}
Compression=lzma
SolidCompression=yes
AppMutex={code:GetAppMutex}
SetupMutex={#AppMutex}Setup
SetupIconFile={#ResourcesDir}\{#AppIconName}.ico
UninstallDisplayIcon={app}\{#AppExeName}.exe
ChangesEnvironment=true
ChangesAssociations=true
MinVersion=10.0.17763
SourceDir={#SourceDir}
AppVersion={#Version}
VersionInfoVersion={#Version}
ShowLanguageDialog=auto
WizardStyle=modern

CloseApplications=force

#if GetEnv("CI") != ""
SignTool=Defaultsign
#endif

DefaultDirName={autopf}\{#AppName}
PrivilegesRequired=lowest

ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl,{#ResourcesDir}\messages\en.isl"

[UninstallDelete]
Type: filesandordirs; Name: "{app}\logs"
Type: filesandordirs; Name: "{app}\cache"
Type: filesandordirs; Name: "{app}\updates"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "associatewithfiles"; Description: "{cm:AssociateWithFiles,{#AppDisplayName}}"; GroupDescription: "{cm:Other}"
Name: "addtopath"; Description: "{cm:AddToPath}"; GroupDescription: "{cm:Other}"

[Dirs]
Name: "{app}"; AfterInstall: DisableAppDirInheritance

[Files]
Source: "{#ResourcesDir}\ZQLZ.exe"; DestDir: "{code:GetInstallDir}"; Flags: ignoreversion
Source: "{#ResourcesDir}\bin\*"; DestDir: "{code:GetInstallDir}\bin"; Flags: ignoreversion recursesubdirs createallsubdirs; Check: DirExists(ExpandConstant('{#ResourcesDir}\bin'))

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExeName}.exe"; AppUserModelID: "{#AppUserId}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}.exe"; Tasks: desktopicon; AppUserModelID: "{#AppUserId}"

[Run]
Filename: "{app}\{#AppExeName}.exe"; Description: "{cm:LaunchProgram,{#AppName}}"; Flags: nowait postinstall; Check: WizardNotSilent

[Registry]
; SQL file association
Root: HKCU; Subkey: "Software\Classes\.sql\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.sql\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.sql"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sql"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,SQL}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sql"; ValueType: string; ValueName: "AppUserModelID"; ValueData: "{#AppUserId}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sql\DefaultIcon"; ValueType: none; Flags: deletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sql\shell\open"; ValueType: string; ValueName: "Icon"; ValueData: """{app}\{#AppExeName}.exe"""; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sql\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

; SQLite file association
Root: HKCU; Subkey: "Software\Classes\.sqlite\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.sqlite\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.sqlite"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sqlite"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,SQLite Database}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sqlite"; ValueType: string; ValueName: "AppUserModelID"; ValueData: "{#AppUserId}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sqlite\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

Root: HKCU; Subkey: "Software\Classes\.sqlite3\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.sqlite3\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.sqlite3"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sqlite3"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,SQLite Database}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.sqlite3\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

Root: HKCU; Subkey: "Software\Classes\.db\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.db\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.db"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.db"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,Database}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.db\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

; DuckDB file association
Root: HKCU; Subkey: "Software\Classes\.duckdb\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.duckdb\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.duckdb"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.duckdb"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,DuckDB Database}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.duckdb\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

; CSV file association
Root: HKCU; Subkey: "Software\Classes\.csv\OpenWithProgids"; ValueType: none; ValueName: "{#RegValueName}"; Flags: deletevalue uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\.csv\OpenWithProgids"; ValueType: string; ValueName: "{#RegValueName}.csv"; ValueData: ""; Flags: uninsdeletevalue; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.csv"; ValueType: string; ValueName: ""; ValueData: "{cm:SourceFile,CSV}"; Flags: uninsdeletekey; Tasks: associatewithfiles
Root: HKCU; Subkey: "Software\Classes\{#RegValueName}.csv\shell\open\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}.exe"" ""%1"""; Tasks: associatewithfiles

; PATH environment variable
Root: HKCU; Subkey: "Environment"; ValueType: expandsz; ValueName: "Path"; ValueData: "{olddata};{app}"; Tasks: addtopath; Check: NeedsAddPath(ExpandConstant('{app}'))

[Code]
function GetInstallDir(Param: String): String;
begin
  Result := ExpandConstant('{app}');
end;

function GetAppMutex(Param: String): String;
begin
  Result := '{#AppMutex}';
end;

function WizardNotSilent: Boolean;
begin
  Result := not WizardSilent;
end;

procedure DisableAppDirInheritance;
var
  ResultCode: Integer;
begin
  // Disable inheritance on app directory for security
  Exec('icacls.exe', '"' + ExpandConstant('{app}') + '" /inheritance:d', '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

function NeedsAddPath(Param: string): boolean;
var
  OrigPath: string;
begin
  if not RegQueryStringValue(HKEY_CURRENT_USER, 'Environment', 'Path', OrigPath) then
  begin
    Result := True;
    exit;
  end;
  Result := Pos(';' + Param + ';', ';' + OrigPath + ';') = 0;
end;
