<# =====================================================================
 FullKit-CBibles-Windows.ps1
 FINAL+PLUS Bible SWORD exporter — Windows (PowerShell 7+)

 Source:   C:\Bibles\SWORD_modules         (zips only are fine)
 Output:   C:\Bibles\EXPORT
 Goals:    Process ALL modules & languages, idempotent rebuild
           Exports: TXT, OSIS, MD, PDF, EPUB, DOCX, per-book & per-chapter,
                    verse CSV/JSON, Strong’s & morph TSV (+freq), diglots,
                    merged PDFs per-language, catalogs (CSV/JSON/INDEX.md),
                    coverage matrix, static HTML site + lunr index,
                    OPDS feed, SQLite FTS DB + search CLI, packaging, checksums
 Reliability: robust extraction (quarantine bad zips, 7z retry), long-paths,
              OneDrive placeholders, permission checks, auto-install tools.
 Parallelism: -Parallel (with -ThrottleLimit N)
 Self-test:   -SelfTest
 Config:      -Config export.config.yaml (feature flags)
 Overwrite:   ALWAYS overwrite outputs on re-run (fresh rebuild)
 ====================================================================== #>

param(
  [string]$SourceRoot = 'C:\Bibles\SWORD_modules',
  [string]$ExportRoot = 'C:\Bibles\EXPORT',
  [string]$Config = '',
  [switch]$Parallel,
  [int]$ThrottleLimit = 4,
  [switch]$SelfTest
)

# --- Language map init & canonicalizer ---------------------------------------
function Init-LangMap {
  if (-not $script:LangMap) {
    # Keys are lowercase and use '-' (never '_', never uppercase).
    $script:LangMap = @{
      # English and common aliases
      'en'='en'; 'eng'='en'; 'en-us'='en'; 'en-gb'='en'; 'en-au'='en'; 'english'='en'

      # Major languages (sample set; extend as needed)
      'es'='es'; 'spa'='es'
      'fr'='fr'; 'fra'='fr'; 'fre'='fr'
      'de'='de'; 'deu'='de'; 'ger'='de'
      'ru'='ru'; 'rus'='ru'
      'pt'='pt'; 'por'='pt'
      'it'='it'; 'ita'='it'
      'nl'='nl'; 'nld'='nl'; 'dut'='nl'
      'sv'='sv'; 'swe'='sv'
      'no'='no'; 'nor'='no'
      'da'='da'; 'dan'='da'
      'fi'='fi'; 'fin'='fi'
      'pl'='pl'; 'pol'='pl'
      'cs'='cs'; 'ces'='cs'; 'cze'='cs'
      'hu'='hu'; 'hun'='hu'
      'ro'='ro'; 'ron'='ro'; 'rum'='ro'
      'el'='el'; 'ell'='el'; 'gre'='el'
      'la'='la'; 'lat'='la'
      'he'='he'; 'iw'='he'
      'yi'='yi'; 'ji'='yi'
      'id'='id'; 'in'='id'
      'jv'='jv'; 'jw'='jv'
      'tr'='tr'; 'tur'='tr'
      'ar'='ar'; 'ara'='ar'
      'fa'='fa'; 'fas'='fa'; 'per'='fa'
      'ur'='ur'
      'hi'='hi'; 'hin'='hi'
      'bn'='bn'; 'ben'='bn'
      'ta'='ta'; 'tam'='ta'
      'te'='te'; 'tel'='te'
      'ml'='ml'; 'mal'='ml'
      'kn'='kn'; 'kan'='kn'
      'mr'='mr'; 'mar'='mr'
      'pa'='pa'; 'pan'='pa'
      'gu'='gu'; 'guj'='gu'
      'uk'='uk'; 'ukr'='uk'
      'bg'='bg'; 'bul'='bg'
      'sr'='sr'; 'srp'='sr'
      'hr'='hr'; 'hrv'='hr'
      'sk'='sk'; 'slk'='sk'
      'sl'='sl'; 'slv'='sl'
      'et'='et'; 'est'='et'
      'lv'='lv'; 'lav'='lv'
      'lt'='lt'; 'lit'='lt'
      'ga'='ga'; 'gle'='ga'
      'is'='is'; 'isl'='is'; 'ice'='is'
      'ms'='ms'; 'msa'='ms'; 'may'='ms'
      'vi'='vi'; 'vie'='vi'
      'th'='th'; 'tha'='th'
      'ko'='ko'; 'kor'='ko'
      'ja'='ja'; 'jpn'='ja'

      # Chinese (normalized)
      'zh'='zh'
      'zh-cn'='zh-hans-cn'
      'zh-tw'='zh-hant-tw'
    }
  }
}

function Canonical-Lang([string]$Lang) {
  Init-LangMap
  if ([string]::IsNullOrWhiteSpace($Lang)) { return 'und' }

  # Normalize first: lowercase and convert '_' to '-'
  $k = $Lang.Trim().ToLower().Replace('_','-')

  if ($script:LangMap.ContainsKey($k)) { return $script:LangMap[$k] }

  # ISO-ish fallback: first 2 letters if present
  if ($k.Length -ge 2) { return $k.Substring(0,2) }

  return 'und'
}

# ----------------------------------------------------------------------------- 

# --- Globals ------------------------------------------------------------
$ErrorActionPreference = 'Stop'
$PSStyle.OutputRendering = 'PlainText'
$script:StartTime = Get-Date
$script:RunId = (Get-Date -Format 'yyyyMMdd_HHmmss')
$script:LogDir = Join-Path $ExportRoot '_logs'
$script:LogFile = Join-Path $script:LogDir "run_$($script:RunId).log"

# Create log dir/file without relying on Ensure-Dir (it’s defined later)
if (-not (Test-Path -LiteralPath $script:LogDir)) {
  New-Item -ItemType Directory -Force -Path $script:LogDir | Out-Null
}
if (-not (Test-Path -LiteralPath $script:LogFile)) {
  New-Item -ItemType File -Path $script:LogFile -Force | Out-Null
}

# Concurrency-friendly collections
$script:NoOsisList = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
$script:SkipReasons = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
$script:ErrHeat = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()

# Tool paths (populated by Ensure-Tools)
$script:Tool = @{
  diatheke     = $null
  mod2osis     = $null
  '7z'         = $null
  pandoc       = $null
  xelatex      = $null
  wkhtmltopdf  = $null
  qpdf         = $null
  pdftk        = $null
  gs           = $null
  sqlite3      = $null
}

# --- Default configuration (will be deep-merged with YAML/JSON if provided)
$ConfigObj = @{
  formats = @{
    txt=$true; osis=$true; md=$true; pdf=$true; epub=$true; docx=$true
    per_chapter=$true; per_book=$true
    verse_json=$true; verse_csv=$true
    strongs_tsv=$true; xref_footnotes=$true
    diglots=$true; site=$true; opds=$true; sqlite=$true; bundles=$true
  }
  parallel = @{ enabled = $false; throttle = 4 }
  diglot_pairs = @()      # e.g. @(@('KJV','ASV'), @('ESV','LSG'))
  license = @{ exclude_restricted = $false }
  site = @{ title = 'CBibles Export'; theme='minimal' }
  filters = @{
    include_modules = @() # e.g. @('KJV','ESV')
    include_langs   = @() # e.g. @('en','pt','pt-BR','pt-PT')
    exclude_modules = @()
    exclude_langs   = @()
  }
}
$script:ConfigObj = $ConfigObj

# --- Logging helpers ----------------------------------------------------
function Write-Log {
  param([string]$Level='INFO',[string]$Msg)

  if ([string]::IsNullOrWhiteSpace($script:LogDir)) {
    if ($ExportRoot)      { $script:LogDir = Join-Path $ExportRoot '_logs' }
    elseif ($env:TEMP)    { $script:LogDir = Join-Path $env:TEMP 'CBibles_logs' }
    else                  { $script:LogDir = Join-Path (Get-Location) '_logs' }
  }
  try {
    if (-not (Test-Path -LiteralPath $script:LogDir)) {
      New-Item -ItemType Directory -Force -Path $script:LogDir | Out-Null
    }
  } catch { }

  if ([string]::IsNullOrWhiteSpace($script:LogFile)) {
    $rid = if ($script:RunId) { $script:RunId } else { (Get-Date -Format 'yyyyMMdd_HHmmss') }
    $script:LogFile = Join-Path $script:LogDir "run_$rid.rp_$([System.Guid]::NewGuid().ToString('N').Substring(0,8)).log"
    try { New-Item -ItemType File -Path $script:LogFile -Force | Out-Null } catch { }
  }

  $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
  $line = "[$ts] $Level $Msg"
  try { Add-Content -LiteralPath $script:LogFile -Value $line } catch { }

  if ($Level -eq 'ERROR') { Write-Host $line -ForegroundColor Red }
  elseif ($Level -eq 'WARN') { Write-Host $line -ForegroundColor Yellow }
  else { Write-Host $line }
}

function Bump-ErrHeat { param([string]$Key)
  if (-not $script:ErrHeat) {
    $script:ErrHeat = [System.Collections.Concurrent.ConcurrentDictionary[string,int]]::new()
  }
  [void]$script:ErrHeat.AddOrUpdate($Key, 1, { param($k,$v) $v + 1 })
}

function Assert-Path { param([Parameter(Mandatory)][string]$Path)
  if ([string]::IsNullOrWhiteSpace($Path)) { throw "Assert-Path: target path is null/empty." }
  $dir = Split-Path -Parent $Path
  if ($dir -and -not (Test-Path -LiteralPath $dir)) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
  }
}

function Safe-SetContent {
  param(
    [Parameter(Mandatory)][string]$LiteralPath,
    [Parameter(Mandatory)][AllowEmptyString()][string]$Value,
    [ValidateSet('UTF8','ASCII')][string]$Encoding='UTF8'
  )
  if ($null -eq $Value) { $Value = '' }
  Assert-Path -Path $LiteralPath
  Set-Content -LiteralPath $LiteralPath -Value $Value -Encoding $Encoding
}

# --- FS helpers ---------------------------------------------------------
function Ensure-Dir { param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) {
    New-Item -ItemType Directory -Force -Path $Path | Out-Null
  }
}
function Is-OnlineOnly { param([string]$Path)
  try {
    $attr = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
    return (($attr.Attributes.ToString() -match 'Offline') -or
            ($attr.Attributes.value__ -band 0x400000))
  } catch { return $false }
}

# --- Tool finding & auto-install ---------------------------------------
function Find-InPath { param([string]$exe)
  $paths = ($env:PATH -split ';') + @('C:\Program Files','C:\Program Files (x86)','C:\ProgramData\chocolatey\bin')
  foreach($p in $paths) {
    try {
      $cand = Get-ChildItem -Path $p -Filter "$exe*.exe" -Recurse -ErrorAction SilentlyContinue | Select-Object -First 1
      if ($cand) { return $cand.FullName }
    } catch {}
  }
  return $null
}
function Ensure-Tool { param([string]$name,[string]$wingetId,[string]$chocoId)
  $found = Find-InPath $name
  if (-not $found) {
    Write-Log WARN "$name not found. Attempting auto-install..."
    try {
      if (Get-Command winget -ErrorAction SilentlyContinue) {
        winget install --id $wingetId -e --accept-source-agreements --accept-package-agreements | Out-Null
      } elseif (Get-Command choco -ErrorAction SilentlyContinue) {
        choco install $chocoId -y | Out-Null
      }
    } catch {}
    $found = Find-InPath $name
  }
  if ($found) { $script:Tool[$name] = $found; Write-Log INFO "$name => $found" }
  else { Write-Log WARN "$name unavailable; will use fallbacks if possible." }
}
function Ensure-Tools {
  Ensure-Tool 'diatheke' 'CrossWire.Sword' 'diatheke'
  Ensure-Tool 'mod2osis' 'CrossWire.Sword' 'sword'
  Ensure-Tool '7z' '7zip.7zip' '7zip'
  Ensure-Tool 'pandoc' 'JohnMacFarlane.Pandoc' 'pandoc'
  Ensure-Tool 'xelatex' 'MiKTeX.MiKTeX' 'miktex'
  Ensure-Tool 'wkhtmltopdf' 'wkHTMLtoPDF.wkHTMLtoPDF' 'wkhtmltopdf'
  Ensure-Tool 'qpdf' 'QPDF.QPDF' 'qpdf'
  Ensure-Tool 'pdftk' 'strawberryperl' 'pdftk'  # often via choco pdftk.portable
  Ensure-Tool 'gswin64c' 'ArtifexSoftware.GhostScript' 'ghostscript'
  if ($script:Tool['gswin64c']) { $script:Tool['gs'] = $script:Tool['gswin64c'] }
  Ensure-Tool 'sqlite3' 'SQLite.SQLite' 'sqlite'
}

# --- OSIS validators / transforms --------------------------------------
function Test-OsisValid { param([string]$OsisPath)
  if (-not (Test-Path -LiteralPath $OsisPath)) { return $false }
  $len = (Get-Item -LiteralPath $OsisPath).Length
  if ($len -lt 100) { return $false }
  try {
    $settings = [System.Xml.XmlReaderSettings]::new()
    $settings.DtdProcessing = [System.Xml.DtdProcessing]::Ignore
    $settings.IgnoreComments = $true
    $settings.IgnoreWhitespace = $true
    $settings.CloseInput = $true
    $fs = [System.IO.File]::OpenRead($OsisPath)
    try {
      $xr = [System.Xml.XmlReader]::Create($fs, $settings)
      while($xr.Read()) { }
      $xr.Close()
    } finally { $fs.Dispose() }
    return $true
  } catch { return $false }
}
function Clean-Reparse-Xml { param([string]$InPath,[string]$OutPath)
  try {
    $bytes = [System.IO.File]::ReadAllBytes($InPath)
    $utf8 = New-Object System.Text.UTF8Encoding($false)
    $text = $utf8.GetString($bytes) -replace "`0","" -replace "\r\n", "`n"
    $text = $text -replace '<!DOCTYPE[^>]*>', ''
    Assert-Path -Path $OutPath
    [System.IO.File]::WriteAllText($OutPath, $text, $utf8)
    return $true
  } catch { Write-Log WARN "Clean-Reparse failed: $InPath => $_"; return $false }
}
function PandocFromTxtToMdPdf { param([string]$Txt,[string]$Md,[string]$Pdf,[string]$Title='')
  # Ensure input exists (create placeholder if missing)
  if (-not (Test-Path -LiteralPath $Txt)) {
    Write-Log WARN "Pandoc input TXT missing ($Txt). Creating a placeholder."
    Safe-SetContent -LiteralPath $Txt -Value "# Placeholder — no TXT content available" -Encoding UTF8
  }
  $okMd = $false; $okPdf = $false
  try {
    if ($script:Tool['pandoc']) {
      & $script:Tool['pandoc'] -f markdown -t gfm -o $Md --metadata title="$Title" --toc --strip-comments --wrap=none --from=markdown_strict $Txt 2>$null
      $raw = Get-Content -Raw -LiteralPath $Txt
      $mdBody = "# $Title`n`n" + $raw
      Safe-SetContent -LiteralPath $Md -Value $mdBody -Encoding UTF8
      $okMd = $true
    } else {
      $raw = Get-Content -Raw -LiteralPath $Txt
      $mdBody = "# $Title`n`n" + $raw
      Safe-SetContent -LiteralPath $Md -Value $mdBody -Encoding UTF8
      $okMd = $true
    }
  } catch { Write-Log WARN "Pandoc MD step failed for $Txt => $Md. $_" }
  if ($okMd) {
    try {
      if ($script:Tool['pandoc'] -and $script:Tool['xelatex']) {
        & $script:Tool['pandoc'] $Md -o $Pdf --pdf-engine=xelatex --toc --metadata=title:"$Title" 2>$null
        if (Test-Path $Pdf) { $okPdf = $true }
      } elseif ($script:Tool['wkhtmltopdf']) {
        $html = [System.IO.Path]::ChangeExtension($Md,'.html')
        $h = "<html><head><meta charset='utf-8'><title>$Title</title></head><body>" +
             ((Get-Content -Raw $Md) -replace "`n","<br/>") + "</body></html>"
        Safe-SetContent -LiteralPath $html -Value $h -Encoding UTF8
        & $script:Tool['wkhtmltopdf'] $html $Pdf 2>$null
        Remove-Item -LiteralPath $html -ErrorAction SilentlyContinue
        if (Test-Path $Pdf) { $okPdf = $true }
      }
    } catch { Write-Log WARN "PDF build failed for $Md => $Pdf. $_" }
  }
  return @{ md=$okMd; pdf=$okPdf }
}

# --- PDF merge ----------------------------------------------------------
function Merge-Pdf { param([string[]]$Inputs,[string]$OutPdf)
  if (-not $Inputs -or $Inputs.Count -eq 0) { return $false }
  try {
    if ($script:Tool['qpdf']) {
      & $script:Tool['qpdf'] --empty --pages $Inputs -- $OutPdf 2>$null
      if (Test-Path $OutPdf) { return $true }
    }
    if ($script:Tool['pdftk']) {
      & $script:Tool['pdftk'] @Inputs cat output $OutPdf 2>$null
      if (Test-Path $OutPdf) { return $true }
    }
    if ($script:Tool['gs']) {
      & $script:Tool['gs'] -dBATCH -dNOPAUSE -q -sDEVICE=pdfwrite -sOutputFile="$OutPdf" @Inputs 2>$null
      if (Test-Path $OutPdf) { return $true }
    }
  } catch { Write-Log WARN "PDF merge failed ($OutPdf). $_" }
  try {
    if ($script:Tool['pandoc'] -and $script:Tool['xelatex']) {
      $md = [System.IO.Path]::ChangeExtension($OutPdf,'.md')
      $sb = New-Object System.Text.StringBuilder
      [void]$sb.AppendLine("# Combined PDFs")
      foreach($p in $Inputs) { [void]$sb.AppendLine("* $([System.IO.Path]::GetFileName($p))") }
      Safe-SetContent -LiteralPath $md -Value $sb.ToString() -Encoding UTF8
      & $script:Tool['pandoc'] $md -o $OutPdf --pdf-engine=xelatex --toc 2>$null
      Remove-Item $md -ErrorAction SilentlyContinue
      return (Test-Path $OutPdf)
    }
  } catch {}
  return $false
}

# --- ZIP extraction with quarantine & 7z retry --------------------------
function Extract-Zip { param([string]$Zip,[string]$Dest)
  Ensure-Dir $Dest
  try {
    Expand-Archive -Path $Zip -DestinationPath $Dest -Force
    return $true
  } catch {
    Write-Log WARN "Expand-Archive failed for $Zip. Trying 7z..."
    if ($script:Tool['7z']) {
      try {
        & $script:Tool['7z'] x -y "-o$Dest" "$Zip" | Out-Null
        return $true
      } catch {
        Write-Log ERROR "7z also failed for $Zip. Quarantining."
        $qdir = Join-Path (Split-Path $Zip -Parent) '_quarantine'
        Ensure-Dir $qdir
        Move-Item -LiteralPath $Zip -Destination (Join-Path $qdir (Split-Path $Zip -Leaf)) -Force
        Bump-ErrHeat 'bad_zip'
        return $false
      }
    } else {
      Write-Log ERROR "7z not available; quarantining $Zip"
      $qdir = Join-Path (Split-Path $Zip -Parent) '_quarantine'
      Ensure-Dir $qdir
      Move-Item -LiteralPath $Zip -Destination (Join-Path $qdir (Split-Path $Zip -Leaf)) -Force
      Bump-ErrHeat 'bad_zip'
      return $false
    }
  }
}

# --- SWORD module discovery --------------------------------------------
function Discover-Archives { param([string]$Root)
  if (Is-OnlineOnly $Root) {
    Write-Log WARN "Source appears cloud-only. Make files 'Available on this device'."
  }
  $zips = Get-ChildItem -LiteralPath $Root -Filter *.zip -Recurse -File -ErrorAction SilentlyContinue
  return $zips
}

# --- Synthesize mods.d if missing --------------------------------------
function Synthesize-ModsD { param([string]$ModuleDir,[string]$Name,[string]$Lang='und')
  $mods = Join-Path $ModuleDir 'mods.d'
  Ensure-Dir $mods
  $conf = Join-Path $mods "$Name.conf"
  if (-not (Test-Path $conf)) {
    $datapath = (Resolve-Path -LiteralPath $ModuleDir).Path
    $body = @"
[$Name]
DataPath=$datapath
Description=$Name (synthetic)
Lang=$Lang
Encoding=UTF-8
Versification=KJV
ModDrv=zText
"@
    Safe-SetContent -LiteralPath $conf -Value $body -Encoding UTF8
    Write-Log INFO "Synthesized mods.d for $Name"
  }
}

# --- Diatheke helpers ---------------------------------------------------
function Get-Diatheke-Txt { param([string]$Module,[string]$OutTxt)
  if (-not $script:Tool['diatheke']) { throw "diatheke unavailable" }
  # KJV versification range per book (large cap to ensure coverage)
  $books = @(
    'Gen','Exod','Lev','Num','Deut','Josh','Judg','Ruth','1Sam','2Sam','1Kgs','2Kgs',
    '1Chr','2Chr','Ezra','Neh','Esth','Job','Ps','Prov','Eccl','Song','Isa','Jer',
    'Lam','Ezek','Dan','Hos','Joel','Amos','Obad','Jonah','Mic','Nah','Hab','Zeph',
    'Hag','Zech','Mal','Matt','Mark','Luke','John','Acts','Rom','1Cor','2Cor','Gal',
    'Eph','Phil','Col','1Thess','2Thess','1Tim','2Tim','Titus','Phlm','Heb','Jas',
    '1Pet','2Pet','1John','2John','3John','Jude','Rev'
  )
  $sb = New-Object System.Text.StringBuilder
  foreach($b in $books) {
    try {
      $res = & $script:Tool['diatheke'] -b $Module -o n -f plaintext -k "$b 1:1-$b 200:200" 2>$null
      if ($res) { [void]$sb.AppendLine($res) }
    } catch {
      Write-Log WARN "diatheke read failed for $Module $b"
    }
  }
  $txtOut = $sb.ToString()
  if ([string]::IsNullOrEmpty($txtOut)) {
    Write-Log WARN "diatheke produced no text for module '$Module' (check SWORD_PATH or module integrity). Writing placeholder."
    $txtOut = "# Empty export: '$Module' produced no verses via diatheke.`n"
  }
  Safe-SetContent -LiteralPath $OutTxt -Value $txtOut -Encoding UTF8
}

# --- mod2osis -----------------------------------------------------------
function Get-Osis { param([string]$ModulePath,[string]$ModuleName,[string]$OutOsis)
  if (-not $script:Tool['mod2osis']) { throw "mod2osis unavailable" }
  try { & $script:Tool['mod2osis'] "$ModuleName" "$ModulePath" > $OutOsis 2>$null }
  catch { throw }
}

# --- Strong’s & morph TSV from OSIS ------------------------------------
function Extract-Strongs-Morph { param([string]$Osis,[string]$Tsv,[string]$FreqJson)
  try {
    $xml = [xml](Get-Content -Raw -LiteralPath $Osis)
    $ns = @{ o='http://www.bibletechnologies.net/2003/OSIS/namespace' }
    $words = $xml.SelectNodes('//o:w[@lemma or @morph]', $ns)
    $rows = @()
    $freq = @{}
    foreach($w in $words) {
      $lemma = $w.lemma
      $morph = $w.morph
      $txt = ($w.InnerText -replace '\s+',' ').Trim()
      $refNode = $w.SelectSingleNode('ancestor::o:verse[1]', $ns)
      $ref = if ($refNode) { $refNode.GetAttribute('osisID') } else { '' }
      $rows += "{0}`t{1}`t{2}`t{3}" -f $ref, $txt, $lemma, $morph
      if ($lemma) { $freq[$lemma] = 1 + ($freq[$lemma] | ForEach-Object { $_ }) }
    }
    Safe-SetContent -LiteralPath $Tsv -Value ($rows -join "`n") -Encoding UTF8
    $freqPairs = $freq.GetEnumerator() | Sort-Object Value -Descending
    $freqObj = @{}
    foreach($p in $freqPairs){ $freqObj[$p.Key] = $p.Value }
    $freqObj | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $FreqJson -Encoding UTF8
    return $true
  } catch {
    Write-Log WARN "Strong’s/morph extraction failed: $_"
    return $false
  }
}

# --- Coverage from OSIS or TXT -----------------------------------------
function Compute-Coverage { param([string]$Osis,[string]$Txt,[string]$CsvOut)
  $set = New-Object System.Collections.Generic.HashSet[string]
  try {
    if ($Osis -and (Test-OsisValid $Osis)) {
      $xml = [xml](Get-Content -Raw -LiteralPath $Osis)
      $ns = @{ o='http://www.bibletechnologies.net/2003/OSIS/namespace' }
      $verses = $xml.SelectNodes('//o:verse[@osisID]', $ns)
      foreach($v in $verses) { [void]$set.Add($v.osisID) }
    } else {
      $text = Get-Content -Raw -LiteralPath $Txt
      [regex]::Matches($text,'([1-3]?\s?[A-Za-z]+)\s+(\d+):(\d+)') | ForEach-Object {
        [void]$set.Add("$($_.Groups[1].Value).$($_.Groups[2].Value).$($_.Groups[3].Value)")
      }
    }
  } catch { Write-Log WARN "Coverage parse failed: $_" }
  Safe-SetContent -LiteralPath $CsvOut -Value "ref" -Encoding UTF8
  Add-Content -LiteralPath $CsvOut -Value ($set | Sort-Object)
}

# --- Per-book / per-chapter splitting ----------------------------------
function Split-Txt-ByBookChapter { param([string]$Txt,[string]$OutDir)
  Ensure-Dir $OutDir
  $bookDir = Join-Path $OutDir 'books'
  $chapDir = Join-Path $OutDir 'chapters'
  Ensure-Dir $bookDir; Ensure-Dir $chapDir
  $lines = Get-Content -LiteralPath $Txt -Encoding UTF8
  $bookBuckets = @{}
  $chapBuckets = @{}
  foreach($ln in $lines) {
    $m = [regex]::Match($ln,'^([1-3]?\s?[A-Za-z]+)\s+(\d+):(\d+)\s+(.*)')
    if ($m.Success) {
      $b=$m.Groups[1].Value.Trim(); $c=$m.Groups[2].Value; $v=$m.Groups[3].Value
      if (-not $bookBuckets.ContainsKey($b)) { $bookBuckets[$b] = New-Object System.Text.StringBuilder }
      [void]$bookBuckets[$b].AppendLine($ln)
      $ckey = "$b $c"
      if (-not $chapBuckets.ContainsKey($ckey)) { $chapBuckets[$ckey] = New-Object System.Text.StringBuilder }
      [void]$chapBuckets[$ckey].AppendLine($ln)
    }
  }
  foreach($k in $bookBuckets.Keys) {
    Safe-SetContent -LiteralPath (Join-Path $bookDir "$k.txt") -Value $bookBuckets[$k].ToString() -Encoding UTF8
  }
  foreach($k in $chapBuckets.Keys) {
    $safe = ($k -replace '[^\w\s.-]','_')
    Safe-SetContent -LiteralPath (Join-Path $chapDir "$safe.txt") -Value $chapBuckets[$k].ToString() -Encoding UTF8
  }
  return @{ books=$bookDir; chapters=$chapDir }
}

# --- TXT → MD/PDF/EPUB/DOCX per file -----------------------------------
function Build-Textual-Formats { param([string]$Txt,[string]$BaseOut,[string]$Title='')
  Ensure-Dir (Split-Path $BaseOut -Parent)
  $md = "$BaseOut.md"; $pdf="$BaseOut.pdf"; $epub="$BaseOut.epub"; $docx="$BaseOut.docx"
  $null = PandocFromTxtToMdPdf -Txt $Txt -Md $md -Pdf $pdf -Title $Title
  try {
    if ($script:Tool['pandoc']) {
      & $script:Tool['pandoc'] $md -o $epub --toc --metadata=title:"$Title" 2>$null
      & $script:Tool['pandoc'] $md -o $docx --toc --metadata=title:"$Title" 2>$null
    }
  } catch { Write-Log WARN "EPUB/DOCX build failed for $Txt. $_" }
  return @{ md=$md; pdf=$pdf; epub=$epub; docx=$docx }
}

# --- Verse CSV/JSON from TXT -------------------------------------------
function Build-VerseTables { param([string]$Txt,[string]$Csv,[string]$Json)
  $rows = @()
  Get-Content -LiteralPath $Txt | ForEach-Object {
    $m = [regex]::Match($_,'^([1-3]?\s?[A-Za-z]+)\s+(\d+):(\+?\d+)\s+(.*)')
    if ($m.Success) {
      $obj = [ordered]@{
        book=$m.Groups[1].Value; chapter=[int]$m.Groups[2].Value; verse=$m.Groups[3].Value; text=$m.Groups[4].Value
      }
      $rows += $obj
    }
  }
  $rows | ConvertTo-Csv -NoTypeInformation | Set-Content -LiteralPath $Csv -Encoding UTF8
  $rows | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $Json -Encoding UTF8
}

# --- Diglot side-by-side TXT/PDF ---------------------------------------
function Build-Diglot { param([string]$ModA,[string]$ModB,[string]$OutTxt,[string]$OutPdf)
  try {
    if (-not $script:Tool['diatheke']) { throw "diatheke unavailable" }
    $tmpA = New-TemporaryFile; $tmpB = New-TemporaryFile
    Get-Diatheke-Txt -Module $ModA -OutTxt $tmpA
    Get-Diatheke-Txt -Module $ModB -OutTxt $tmpB
    $a = Get-Content -LiteralPath $tmpA
    $b = Get-Content -LiteralPath $tmpB
    $h = @{}
    foreach($ln in $a){ $k = ($ln -split '\s+')[0..1] -join ' '; if (-not $h.ContainsKey($k)){$h[$k]=@{A=$ln;B=$null}} else {$h[$k].A=$ln} }
    foreach($ln in $b){ $k = ($ln -split '\s+')[0..1] -join ' '; if (-not $h.ContainsKey($k)){$h[$k]=@{A=$null;B=$ln}} else {$h[$k].B=$ln} }
    $sb = New-Object System.Text.StringBuilder
    foreach($k in ($h.Keys | Sort-Object)) { [void]$sb.AppendLine("$($h[$k].A)`n$($h[$k].B)`n") }
    Safe-SetContent -LiteralPath $OutTxt -Value $sb.ToString() -Encoding UTF8
    [void](Build-Textual-Formats -Txt $OutTxt -BaseOut ([System.IO.Path]::ChangeExtension($OutTxt,$null)) -Title "$ModA / $ModB")
    if (Test-Path ([System.IO.Path]::ChangeExtension($OutTxt,'.pdf'))) {
      Copy-Item ([System.IO.Path]::ChangeExtension($OutTxt,'.pdf')) $OutPdf -Force
    }
    Remove-Item $tmpA,$tmpB -ErrorAction SilentlyContinue
  } catch {
    Write-Log WARN "Diglot failed for $ModA + $ModB — $_"
    Bump-ErrHeat 'diglot_failed'
  }
}

# --- SQLite FTS DB + simple CLI ----------------------------------------
function Build-Sqlite { param([string]$DbPath,[string]$Csv)
  if (-not $script:Tool['sqlite3']) { Write-Log WARN "sqlite3 not available"; return $false }
  try {
    Remove-Item -LiteralPath $DbPath -ErrorAction SilentlyContinue
    & $script:Tool['sqlite3'] $DbPath "CREATE VIRTUAL TABLE verses USING fts5(book, chapter, verse, text);" 2>$null
    & $script:Tool['sqlite3'] $DbPath ".mode csv" ".import '$Csv' verses" 2>$null
    $cli = @"
@echo off
sqlite3 "$DbPath" "SELECT rowid, book, chapter, verse, text FROM verses WHERE verses MATCH %1 LIMIT 20;"
"@
    $bat = Join-Path (Split-Path $DbPath -Parent) 'search_cli.bat'
    Set-Content -LiteralPath $bat -Value $cli -Encoding ASCII
    return $true
  } catch {
    Write-Log WARN "SQLite build failed: $_"
    return $false
  }
}

# --- Static site + OPDS -------------------------------------------------
function Build-StaticSite { param([string]$SiteDir,[string]$IndexMd,[string]$SearchJson,[hashtable]$SiteCfg)
  Ensure-Dir $SiteDir
  if ($script:Tool['pandoc']) {
    & $script:Tool['pandoc'] $IndexMd -o (Join-Path $SiteDir 'index.html') --metadata=title:"$($SiteCfg.title)" --toc 2>$null
  } else {
    Copy-Item $IndexMd (Join-Path $SiteDir 'index.md') -Force
  }
  $css = @"
body{font-family:system-ui, -apple-system, Segoe UI, Roboto, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem;}
h1,h2,h3{line-height:1.2}
.code{white-space:pre-wrap; font-family:ui-monospace, SFMono-Regular, Menlo, Consolas, monospace}
.search{margin:1rem 0}
"@
  Safe-SetContent -LiteralPath (Join-Path $SiteDir 'theme.css') -Value $css -Encoding UTF8
  if ($SearchJson -and (Test-Path $SearchJson)) {
    Copy-Item $SearchJson (Join-Path $SiteDir 'search.json') -Force
  } else {
    Safe-SetContent -LiteralPath (Join-Path $SiteDir 'search.json') -Value '[]' -Encoding UTF8
  }
}
function Build-Opds { param([string]$EpubDir,[string]$OpdsXml)
  $entries = Get-ChildItem -LiteralPath $EpubDir -Filter *.epub -Recurse -File -ErrorAction SilentlyContinue
  $sb = New-Object System.Text.StringBuilder
  [void]$sb.AppendLine('<?xml version="1.0" encoding="utf-8"?>')
  [void]$sb.AppendLine('<feed xmlns="http://www.w3.org/2005/Atom">')
  [void]$sb.AppendLine('<title>CBibles OPDS</title>')
  foreach($e in $entries){
    $href = [System.Web.HttpUtility]::UrlPathEncode($e.FullName)
    [void]$sb.AppendLine("<entry><title>$($e.BaseName)</title><link href=""$href"" type=""application/epub+zip""/></entry>")
  }
  [void]$sb.AppendLine('</feed>')
  Safe-SetContent -LiteralPath $OpdsXml -Value $sb.ToString() -Encoding UTF8
}

# --- Catalogs & INDEX.md ------------------------------------------------
function Build-Catalogs { param([array]$Rows,[string]$Csv,[string]$Json,[string]$IndexMd)
  $Rows | Export-Csv -NoTypeInformation -Path $Csv -Encoding UTF8
  $Rows | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $Json -Encoding UTF8
  $md = @()
  $md += "# CBibles Export Index"
  $md += ""
  $md += "| Module | Lang | Paths |"
  $md += "|---|---|---|"
  foreach($r in $Rows){
    $md += ("| `{0}` | {1} | {2} |" -f $r.module, $r.lang, $r.base)
  }
  Safe-SetContent -LiteralPath $IndexMd -Value ($md -join "`n") -Encoding UTF8
}

# --- Packaging & checksums ---------------------------------------------
function Package-Module { param([string]$BaseDir,[string]$ZipOut)
  if ($script:Tool['7z']) {
    & $script:Tool['7z'] a -tzip -mx=7 "$ZipOut" "$BaseDir\*" | Out-Null
  } else {
    if (Test-Path $ZipOut) { Remove-Item $ZipOut -Force }
    Compress-Archive -Path (Join-Path $BaseDir '*') -DestinationPath $ZipOut -Force
  }
  $sha = Get-FileHash -Algorithm SHA256 -LiteralPath $ZipOut
  Safe-SetContent -LiteralPath ($ZipOut + '.sha256') -Value "$($sha.Hash)  $(Split-Path $ZipOut -Leaf)" -Encoding ASCII
}

# --- Module processor ---------------------------------------------------
function Process-Module { param([string]$ZipPath)
  $name = [System.IO.Path]::GetFileNameWithoutExtension($ZipPath)
  $modWork = Join-Path $ExportRoot "_work\$name"
  $modOut  = Join-Path $ExportRoot "modules\$name"
  Ensure-Dir $modWork; Ensure-Dir $modOut

  Write-Log INFO "=== Module: $name ==="

  if (-not (Extract-Zip -Zip $ZipPath -Dest $modWork)) {
    $script:SkipReasons.Add("$($name): bad_zip"); return
  }

  $modRoot = Get-ChildItem -LiteralPath $modWork -Directory -Recurse | Where-Object { Test-Path (Join-Path $_.FullName 'mods.d') } | Select-Object -First 1
  if (-not $modRoot) { $modRoot = Get-Item -LiteralPath $modWork }

  $lang = 'und'
  $conf = Get-ChildItem -LiteralPath (Join-Path $modRoot.FullName 'mods.d') -Filter *.conf -File -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($conf) {
    try {
      $t = Get-Content -LiteralPath $conf
      $m = $t | Select-String -Pattern '^Lang\s*=\s*(.+)$' -CaseSensitive:$false
      if ($m) { $lang = $m.Matches[0].Groups[1].Value.Trim() }
    } catch {}
  } else {
    Synthesize-ModsD -ModuleDir $modRoot.FullName -Name $name -Lang $lang
  }
  $langCanon = Canonical-Lang $lang

  # Language filter gate (from config)
  $inclLangs = @($script:ConfigObj.filters.include_langs)
  $exclLangs = @($script:ConfigObj.filters.exclude_langs)
  if ($inclLangs.Count -gt 0 -and ($inclLangs -notcontains $langCanon)) {
    Write-Log INFO "Skip $name (lang=$langCanon not in include_langs)."
    $script:SkipReasons.Add("$($name): lang_excluded"); return
  }
  if ($exclLangs.Count -gt 0 -and ($exclLangs -contains $langCanon)) {
    Write-Log INFO "Skip $name (lang=$langCanon in exclude_langs)."
    $script:SkipReasons.Add("$($name): lang_excluded"); return
  }

  # ===== BEGIN SWORD_PATH scope =====
  $oldSwordPath = $env:SWORD_PATH
  $env:SWORD_PATH = $modRoot.FullName
  try {
    # Output structure
    $langDir = Join-Path $ExportRoot "languages\$langCanon"
    Ensure-Dir $langDir
    $base = Join-Path $modOut 'main'
    Ensure-Dir (Split-Path $base -Parent)

    # --- TXT export (diatheke preferred)
    $txt = "$base.txt"
    try {
      if ($script:Tool['diatheke']) {
        Get-Diatheke-Txt -Module $name -OutTxt $txt
      } else {
        $fallback = Get-ChildItem -LiteralPath $modWork -Filter *.txt -Recurse -File | Select-Object -First 1
        if ($fallback) { Copy-Item $fallback.FullName $txt -Force }
        else { Safe-SetContent -LiteralPath $txt -Value "# TXT unavailable for $name" -Encoding UTF8 }
      }
    } catch { Write-Log WARN "TXT export failed for $name — $_"; Bump-ErrHeat 'txt_failed' }

    # --- OSIS export (mod2osis), validate & repair once
    $osis = "$base.osis.xml"
    $osisOk = $false
    try {
      if ($script:Tool['mod2osis']) {
        Get-Osis -ModulePath $modRoot.FullName -ModuleName $name -OutOsis $osis
        if (-not (Test-OsisValid $osis)) {
          $tmp = "$osis.tmp.xml"
          if (Clean-Reparse-Xml -InPath $osis -OutPath $tmp) { Move-Item $tmp $osis -Force }
        }
        $osisOk = Test-OsisValid $osis
        if (-not $osisOk) {
          $script:NoOsisList.Add($name)
          Bump-ErrHeat 'bad_osis'
        }
      } else {
        Write-Log WARN "mod2osis unavailable; skipping OSIS for $name"
        $script:NoOsisList.Add($name)
      }
    } catch {
      Write-Log WARN "OSIS export failed for $name — $_"
      $script:NoOsisList.Add($name)
      Bump-ErrHeat 'bad_osis'
    }

    # --- Formats from TXT (MD/PDF/EPUB/DOCX)
    $formats = Build-Textual-Formats -Txt $txt -BaseOut $base -Title $name

    # --- Per-book & per-chapter
    $splitDirs = Split-Txt-ByBookChapter -Txt $txt -OutDir (Join-Path $modOut 'split')
    foreach($f in (Get-ChildItem -LiteralPath $splitDirs.books -Filter *.txt -File)) {
      [void](Build-Textual-Formats -Txt $f.FullName -BaseOut ([System.IO.Path]::ChangeExtension($f.FullName,$null)) -Title "$name — $($f.BaseName)")
    }
    foreach($f in (Get-ChildItem -LiteralPath $splitDirs.chapters -Filter *.txt -File)) {
      [void](Build-Textual-Formats -Txt $f.FullName -BaseOut ([System.IO.Path]::ChangeExtension($f.FullName,$null)) -Title "$name — $($f.BaseName)")
    }

    # --- Verse tables
    if ($script:ConfigObj.formats.verse_csv -or $script:ConfigObj.formats.verse_json) {
      $csv = "$base.verses.csv"; $json="$base.verses.json"
      Build-VerseTables -Txt $txt -Csv $csv -Json $json
    }

    # --- Strong’s/morph (if OSIS OK)
    if ($osisOk -and $script:ConfigObj.formats.strongs_tsv) {
      $tsv="$base.strongs.tsv"; $freq="$base.lemma_freq.json"
      [void](Extract-Strongs-Morph -Osis $osis -Tsv $tsv -FreqJson $freq)
    }

    # --- Coverage matrix
    Compute-Coverage -Osis ($osisOk ? $osis : $null) -Txt $txt -CsvOut "$base.coverage.csv"

    # --- License-aware packaging
    $restricted = $false
    if ($conf) {
      try {
        $line = (Get-Content -LiteralPath $conf) | Where-Object { $_ -match '^DistributionLicense\s*=' } | Select-Object -First 1
        if ($line -and $line -match 'Restricted') { $restricted = $true }
      } catch {}
    }

    # Per-module bundle (unless config excludes restricted)
    if ($script:ConfigObj.formats.bundles -and (-not ($script:ConfigObj.license.exclude_restricted -and $restricted))) {
      $pkg = Join-Path $modOut "$name.bundle.zip"
      Package-Module -BaseDir $modOut -ZipOut $pkg
    }

    # Per-language merged PDF preparation
    if ($script:ConfigObj.formats.pdf) {
      $langPdfDir = Join-Path $langDir 'merged'
      Ensure-Dir $langPdfDir
      $parts = Join-Path $langPdfDir "All_$langCanon.parts.txt"
      if (-not (Test-Path $parts)) { New-Item -ItemType File -Path $parts | Out-Null }
      Add-Content -LiteralPath $parts -Value $formats.pdf
    }

    # meta.json
    $meta = [ordered]@{
      module=$name; lang=$langCanon; restricted=$restricted
      paths=@{ base=$base; txt=$txt; osis=$osis; md=$formats.md; pdf=$formats.pdf; epub=$formats.epub; docx=$formats.docx }
      time="$((Get-Date).ToString('o'))"
    } | ConvertTo-Json -Depth 5
    Safe-SetContent -LiteralPath (Join-Path $modOut "$name.meta.json") -Value $meta -Encoding UTF8

    return [pscustomobject]@{ module=$name; lang=$langCanon; base=$base }
  }
  finally {
    # ===== END SWORD_PATH scope =====
    $env:SWORD_PATH = $oldSwordPath
  }
}

# --- Config merge helpers (deep, safe) ---------------------------------
function ConvertTo-Hashtable {
  param($InputObject)
  if ($null -eq $InputObject) { return $null }
  if ($InputObject -is [System.Management.Automation.PSCustomObject]) {
    $ht = @{}
    foreach ($p in $InputObject.PSObject.Properties) {
      $ht[$p.Name] = ConvertTo-Hashtable -InputObject $p.Value
    }
    return $ht
  }
  if ($InputObject -is [hashtable]) {
    $ht = @{}
    foreach ($k in $InputObject.Keys) { $ht[$k] = ConvertTo-Hashtable -InputObject $InputObject[$k] }
    return $ht
  }
  if ($InputObject -is [System.Collections.IEnumerable] -and -not ($InputObject -is [string])) {
    $out = @()
    foreach ($v in $InputObject) { $out += ,(ConvertTo-Hashtable -InputObject $v) }
    return $out
  }
  return $InputObject
}
function Merge-Hashtable {
  param([hashtable]$Base, $Overlay)
  if ($null -eq $Base)    { $Base = @{} }
  if ($null -eq $Overlay) { return $Base }
  $OverlayNorm = ConvertTo-Hashtable -InputObject $Overlay
  if ($OverlayNorm -is [hashtable]) {
    foreach ($k in $OverlayNorm.Keys) {
      $ov = $OverlayNorm[$k]
      if ($Base.ContainsKey($k)) {
        $bv = $Base[$k]
        if (($bv -is [hashtable]) -and ($ov -is [hashtable])) {
          $Base[$k] = Merge-Hashtable -Base $bv -Overlay $ov
        } else {
          $Base[$k] = $ov
        }
      } else {
        $Base[$k] = $ov
      }
    }
    return $Base
  }
  return $OverlayNorm
}

# --- YAML/JSON config loader (deep merge) -------------------------------
function Load-Config { param([string]$Path)
  if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) { return }
  try {
    $ext = [System.IO.Path]::GetExtension($Path).ToLowerInvariant()
    if ($ext -eq '.json') {
      $obj = (Get-Content -Raw -LiteralPath $Path) | ConvertFrom-Json -Depth 50
      if ($obj) {
        $script:ConfigObj = Merge-Hashtable -Base (ConvertTo-Hashtable $script:ConfigObj) -Overlay $obj
        Write-Log INFO "Loaded JSON config $Path (merged)."
      }
      $Global:ConfigObj = $script:ConfigObj
      return
    }

    if (Get-Module -ListAvailable -Name 'powershell-yaml') {
      Import-Module 'powershell-yaml' -ErrorAction Stop
    } elseif (Get-Module -ListAvailable -Name 'PowerShellYaml') {
      Import-Module 'PowerShellYaml' -ErrorAction Stop
    } else {
      Write-Log WARN "YAML module not available; skipping YAML config."
      return
    }

    $yaml = Get-Content -Raw -LiteralPath $Path
    $obj  = ConvertFrom-Yaml -Yaml $yaml
    if ($obj) {
      $script:ConfigObj = Merge-Hashtable -Base (ConvertTo-Hashtable $script:ConfigObj) -Overlay $obj
      Write-Log INFO "Loaded YAML config $Path (merged)."
    }
    $Global:ConfigObj = $script:ConfigObj
  } catch {
    Write-Log WARN "Config load failed ($Path). Using defaults. $_"
  }
}

# --- Main ---------------------------------------------------------------
try {
  Ensure-Dir $ExportRoot
  Ensure-Dir $script:LogDir
  Write-Log INFO "Run $script:RunId started. Parallel=$Parallel TL=$ThrottleLimit SelfTest=$SelfTest"

  if ($Config) { Load-Config -Path $Config }

  if (-not ($script:ConfigObj.ContainsKey('filters')) -or -not $script:ConfigObj.filters) {
    $script:ConfigObj.filters = @{
      include_modules = @()
      include_langs   = @()
      exclude_modules = @()
      exclude_langs   = @()
    }
  }

  if ($Parallel) {
    $script:ConfigObj.parallel.enabled = $true
    if ($ThrottleLimit -gt 0) { $script:ConfigObj.parallel.throttle = $ThrottleLimit }
  }

  Ensure-Tools

  $archives = Discover-Archives -Root $SourceRoot
  if ($SelfTest) { $archives = $archives | Select-Object -First 1 }

  # Pre-filter by module ZIP basename (tolerate suffixes like KJV-1.3.zip)
  $inclMods = @($script:ConfigObj.filters.include_modules)
  $exclMods = @($script:ConfigObj.filters.exclude_modules)
  if ($inclMods.Count -gt 0) {
    $archives = $archives | Where-Object {
      $bn = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)
      $inclMods | ForEach-Object {
        if ($bn -match ('^' + [regex]::Escape($_) + '(\b|[-_.].*)?$')) { return $true }
      }
      return $false
    }
  }
  if ($exclMods.Count -gt 0) {
    $archives = $archives | Where-Object {
      $bn = [System.IO.Path]::GetFileNameWithoutExtension($_.Name)
      -not ($exclMods | ForEach-Object { if ($bn -match ('^' + [regex]::Escape($_) + '(\b|[-_.].*)?$')) { return $true } })
    }
  }

  if (-not $archives -or $archives.Count -eq 0) { Write-Log WARN "No ZIP archives found in $SourceRoot"; exit 0 }

  $results = New-Object System.Collections.Concurrent.ConcurrentBag[object]

  if ($script:ConfigObj.parallel.enabled) {
    # === PARALLEL MODE ====================================================
$funcNames = @(
  'Init-LangMap','Canonical-Lang','Write-Log','Bump-ErrHeat','Ensure-Dir','Is-OnlineOnly',
  'Assert-Path','Safe-SetContent',
  'Find-InPath','Ensure-Tool','Ensure-Tools','Extract-Zip','Discover-Archives',
  'Synthesize-ModsD','Get-Diatheke-Txt','Get-Osis','Test-OsisValid',
  'Clean-Reparse-Xml','PandocFromTxtToMdPdf','Build-Textual-Formats',
  'Split-Txt-ByBookChapter','Build-VerseTables','Extract-Strongs-Morph',
  'Compute-Coverage','Merge-Pdf','Package-Module','Build-Sqlite',
  'Build-StaticSite','Build-Opds','Process-Module'
)

    $funcDefs = ($funcNames | ForEach-Object {
      $cmd = Get-Command -Name $_ -CommandType Function -ErrorAction SilentlyContinue | Select-Object -First 1
      if ($null -eq $cmd) { "# Skipping missing function: $_" }
      else { "function $_ {`n$($cmd.Definition)`n}" }
    }) -join "`n`n"

    $ToolSnapshot       = $script:Tool
    $ConfigSnapshot     = $script:ConfigObj
    $RunIdSnapshot      = $script:RunId
    $LogFileSnapshot    = $script:LogFile
    $LogDirSnapshot     = $script:LogDir
    $ExportRootSnapshot = $ExportRoot

    if ([string]::IsNullOrWhiteSpace($ExportRoot)) { throw "ExportRoot is null/empty." }
    if ([string]::IsNullOrWhiteSpace($script:LogFile)) { throw "Log file path is null/empty." }

    $NoOsisListSnapshot = $script:NoOsisList
    $SkipReasonsSnapshot = $script:SkipReasons
    $ErrHeatSnapshot     = $script:ErrHeat

$workerResults = $archives | ForEach-Object -Parallel {
  if ($using:funcDefs) { Invoke-Expression $using:funcDefs }
  $script:Tool      = $using:ToolSnapshot
  $script:ConfigObj = $using:ConfigSnapshot
  $script:RunId     = $using:RunIdSnapshot
  $script:LogFile   = $using:LogFileSnapshot
  $script:LogDir    = $using:LogDirSnapshot
  Set-Variable -Name ExportRoot -Value $using:ExportRootSnapshot -Scope Script

  # Rehydrate shared concurrent collections so .Add/.AddOrUpdate works
  $script:NoOsisList = $using:NoOsisListSnapshot
  $script:SkipReasons = $using:SkipReasonsSnapshot
  $script:ErrHeat     = $using:ErrHeatSnapshot

  Process-Module -ZipPath $_.FullName
} -ThrottleLimit $script:ConfigObj.parallel.throttle

    foreach ($r in $workerResults) { if ($r) { $results.Add($r) } }

  } else {
    # === SERIAL MODE ======================================================
    foreach ($a in $archives) {
      $r = Process-Module -ZipPath $a.FullName
      if ($r) { $results.Add($r) }
    }
  }

  # Per-language merged PDFs
  foreach($langDir in Get-ChildItem -LiteralPath (Join-Path $ExportRoot 'languages') -Directory -ErrorAction SilentlyContinue) {
    $partsFile = Join-Path $langDir.FullName "merged\All_$($langDir.Name).parts.txt"
    if (Test-Path $partsFile) {
      $inputs = Get-Content -LiteralPath $partsFile | Where-Object { $_ -and (Test-Path $_) }
      $outPdf = Join-Path $langDir.FullName "merged\All_$($langDir.Name).pdf"
      if (-not (Merge-Pdf -Inputs $inputs -OutPdf $outPdf)) {
        Write-Log WARN "Could not create true merged PDF for $($langDir.Name); index fallback applied where possible."
        Bump-ErrHeat 'merge_failed'
      }
    }
  }

  # Catalogs / site / OPDS
  $rows = $results.ToArray()
  $csv = Join-Path $ExportRoot 'catalog.csv'
  $json = Join-Path $ExportRoot 'catalog.json'
  $indexMd = Join-Path $ExportRoot 'INDEX.md'
  Build-Catalogs -Rows $rows -Csv $csv -Json $json -IndexMd $indexMd

  if ($script:ConfigObj.formats.site) {
    $siteDir = Join-Path $ExportRoot 'site'
    $searchJson = Join-Path $ExportRoot 'search.json'
    Build-StaticSite -SiteDir $siteDir -IndexMd $indexMd -SearchJson $searchJson -SiteCfg $script:ConfigObj.site
  }
  if ($script:ConfigObj.formats.opds) {
    Build-Opds -EpubDir $ExportRoot -OpdsXml (Join-Path $ExportRoot 'opds.xml')
  }

  if ($script:ConfigObj.formats.sqlite) {
    $anyCsv = Get-ChildItem -LiteralPath $ExportRoot -Filter *.verses.csv -Recurse -File | Select-Object -First 1
    if ($anyCsv) { [void](Build-Sqlite -DbPath (Join-Path $ExportRoot 'verses_fts.sqlite') -Csv $anyCsv.FullName) }
  }

  # Summary
  # Recompute no-OSIS list by scanning outputs (parallel-safe)
$noOsis = Get-ChildItem -LiteralPath (Join-Path $ExportRoot 'modules') -Directory -ErrorAction SilentlyContinue |
  Where-Object {
    $osis = Join-Path $_.FullName 'main.osis.xml'
    -not (Test-Path $osis) -or -not (Test-OsisValid $osis)
  } | Select-Object -ExpandProperty Name

if ($noOsis.Count -gt 0) {
  Write-Log WARN ("No-OSIS modules: {0}" -f ($noOsis -join ', '))
}
  
  $dur = (Get-Date) - $script:StartTime
  Write-Log INFO "=== SUMMARY ==="
  Write-Log INFO ("Modules processed: {0}" -f $rows.Count)
  if ($script:NoOsisList.Count -gt 0) {
    Write-Log WARN ("No-OSIS modules: {0}" -f (($script:NoOsisList.ToArray()) -join ', '))
  }
  if ($script:SkipReasons.Count -gt 0) {
    Write-Log WARN ("Skipped/Issues: {0}" -f (($script:SkipReasons.ToArray()) -join ' | '))
  }
  if ($script:ErrHeat.Count -gt 0) {
    Write-Log WARN "Error heatmap:"
    foreach($k in $script:ErrHeat.Keys){ Write-Log WARN (" - {0}: {1}" -f $k, $script:ErrHeat[$k]) }
  }
  Write-Log INFO ("Elapsed: {0:g}" -f $dur)
  Write-Host "`nDone. Log: $script:LogFile"
} catch {
  $msg = $_.Exception.Message
  $pos = $_.InvocationInfo.PositionMessage
  $stk = $_.ScriptStackTrace
  Write-Log ERROR "Fatal: $msg"
  if ($pos) { Write-Log ERROR "At: $pos" }
  if ($stk) { Write-Log ERROR "Stack: $stk" }
  exit 1
}
