<#
. SYNOPSIS
    Get ADF trigger runs with complete parameter extraction. 
.DESCRIPTION
    Extracts trigger parameters from: 
    1. Trigger definition (default parameters)
    2. Trigger run properties
    3. Pipeline run parameters (which receive trigger parameters)
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$SubscriptionId,
    
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,
    
    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName,
    
    [Parameter(Mandatory=$false)]
    [int]$DaysBack = 7,
    
    [Parameter(Mandatory=$false)]
    [string]$TriggerName,
    
    [Parameter(Mandatory=$false)]
    [string]$ExportCsv,
    
    [Parameter(Mandatory=$false)]
    [string]$ExportJson
)

# Install and import required modules
Write-Host "Checking required modules..." -ForegroundColor Cyan
$requiredModules = @('Az.Accounts', 'Az.DataFactory')

foreach ($module in $requiredModules) {
    if (-not (Get-Module -ListAvailable -Name $module)) {
        Write-Host "Installing $module..." -ForegroundColor Yellow
        Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
    }
    Import-Module $module -ErrorAction Stop
}

Write-Host "✓ Modules loaded" -ForegroundColor Green

# Authenticate
Write-Host "`nAuthenticating to Azure..." -ForegroundColor Cyan
$context = Get-AzContext

if (-not $context) {
    Connect-AzAccount -SubscriptionId $SubscriptionId | Out-Null
} else {
    Set-AzContext -SubscriptionId $SubscriptionId | Out-Null
}

$context = Get-AzContext
Write-Host "✓ Connected as: $($context.Account.Id)" -ForegroundColor Green
Write-Host "  Subscription: $($context. Subscription.Name)" -ForegroundColor Gray

# Calculate time range
$endTime = Get-Date
$startTime = $endTime.AddDays(-$DaysBack)

Write-Host "`nQuery Parameters:" -ForegroundColor Cyan
Write-Host "  Factory:  $DataFactoryName" -ForegroundColor Gray
Write-Host "  Resource Group: $ResourceGroupName" -ForegroundColor Gray
Write-Host "  Time Range: $($startTime.ToString('yyyy-MM-dd HH:mm:ss')) to $($endTime.ToString('yyyy-MM-dd HH:mm:ss'))" -ForegroundColor Gray

# Helper function to extract parameters from trigger definition
function Get-TriggerParameters {
    param($TriggerProperties)
    
    $params = @{}
    
    # Check if trigger has pipelines with parameters
    if ($TriggerProperties. Pipelines) {
        foreach ($pipeline in $TriggerProperties. Pipelines) {
            if ($pipeline.Parameters) {
                foreach ($param in $pipeline.Parameters. GetEnumerator()) {
                    $params[$param.Key] = $param.Value
                }
            }
        }
    }
    
    # For schedule triggers, check for additional properties
    if ($TriggerProperties.PSObject.Properties.Name -contains 'Recurrence') {
        if ($TriggerProperties.Recurrence) {
            $params['ScheduleRecurrence'] = $TriggerProperties.Recurrence | ConvertTo-Json -Compress
        }
    }
    
    # For tumbling window triggers
    if ($TriggerProperties. PSObject.Properties.Name -contains 'Frequency') {
        $params['Frequency'] = $TriggerProperties.Frequency
        $params['Interval'] = $TriggerProperties.Interval
        if ($TriggerProperties.StartTime) {
            $params['StartTime'] = $TriggerProperties.StartTime
        }
        if ($TriggerProperties.EndTime) {
            $params['EndTime'] = $TriggerProperties.EndTime
        }
    }
    
    return $params
}

# Helper function to extract system variables from pipeline runs
function Get-SystemVariablesFromPipelineRun {
    param($PipelineRun)
    
    $systemVars = @{}
    
    if ($PipelineRun.Parameters) {
        foreach ($param in $PipelineRun.Parameters.GetEnumerator()) {
            # Common trigger system variables
            if ($param.Key -match '^(triggerTime|triggerName|windowStart|windowEnd|scheduledTime|triggerType)') {
                $systemVars[$param.Key] = $param.Value
            }
        }
    }
    
    return $systemVars
}

# Get triggers
Write-Host "`nRetrieving triggers..." -ForegroundColor Cyan
try {
    $triggers = Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName -ErrorAction Stop
    
    if ($TriggerName) {
        $triggers = $triggers | Where-Object { $_.Name -eq $TriggerName }
        Write-Host "  Filter:  TriggerName = $TriggerName" -ForegroundColor Gray
    }
    
    if (-not $triggers) {
        Write-Host "✗ No triggers found matching criteria" -ForegroundColor Yellow
        exit 0
    }
    
    Write-Host "✓ Found $($triggers.Count) trigger(s)" -ForegroundColor Green
    
} catch {
    Write-Error "Failed to retrieve triggers: $($_.Exception.Message)"
    exit 1
}

# Query runs for each trigger
Write-Host "`nQuerying trigger runs and extracting parameters..." -ForegroundColor Cyan
$allResults = @()

foreach ($trigger in $triggers) {
    Write-Host "`n  Trigger: $($trigger.Name) [$($trigger.Properties.GetType().Name)]" -ForegroundColor Yellow
    
    # Extract trigger definition parameters
    $triggerDefParams = Get-TriggerParameters -TriggerProperties $trigger. Properties
    
    if ($triggerDefParams. Count -gt 0) {
        Write-Host "    Trigger definition parameters:" -ForegroundColor Gray
        $triggerDefParams. GetEnumerator() | ForEach-Object {
            Write-Host "      • $($_.Key): $($_.Value)" -ForegroundColor DarkGray
        }
    }
    
    try {
        $runs = Get-AzDataFactoryV2TriggerRun `
            -ResourceGroupName $ResourceGroupName `
            -DataFactoryName $DataFactoryName `
            -TriggerName $trigger.Name `
            -TriggerRunStartedAfter $startTime `
            -TriggerRunStartedBefore $endTime `
            -ErrorAction Stop
        
        if ($runs) {
            Write-Host "    ✓ Found $($runs. Count) run(s)" -ForegroundColor Green
            
            foreach ($run in $runs) {
                Write-Host "      Processing run: $($run.TriggerRunId)..." -ForegroundColor DarkGray
                
                # Build result object
                $result = [PSCustomObject]@{
                    TriggerName = $run.TriggerName
                    TriggerRunId = $run.TriggerRunId
                    TriggerType = $run.TriggerType
                    Status = $run.Status
                    TriggerRunTimestamp = $run.TriggerRunTimestamp
                    Message = $run.Message
                    TriggerDefinitionParameters = $triggerDefParams
                    TriggerRunProperties = @{}
                    TriggerSystemVariables = @{}
                    TriggeredPipelines = @()
                }
                
                # Extract trigger run properties
                if ($run.Properties) {
                    foreach ($prop in $run.Properties. GetEnumerator()) {
                        $result.TriggerRunProperties[$prop.Key] = $prop. Value
                    }
                }
                
                # Get pipeline runs to extract parameters that came from trigger
                if ($run.TriggeredPipelines) {
                    foreach ($pipelineEntry in $run.TriggeredPipelines. GetEnumerator()) {
                        $pipelineInfo = [PSCustomObject]@{
                            PipelineName = $pipelineEntry.Key
                            RunId = $pipelineEntry.Value
                            Parameters = @{}
                            TriggerParameters = @{}
                        }
                        
                        # Get pipeline run details
                        try {
                            $pipelineRun = Get-AzDataFactoryV2PipelineRun `
                                -ResourceGroupName $ResourceGroupName `
                                -DataFactoryName $DataFactoryName `
                                -PipelineRunId $pipelineEntry.Value `
                                -ErrorAction Stop
                            
                            if ($pipelineRun) {
                                # All parameters from pipeline run
                                if ($pipelineRun.Parameters) {
                                    foreach ($param in $pipelineRun.Parameters.GetEnumerator()) {
                                        $pipelineInfo.Parameters[$param.Key] = $param.Value
                                        
                                        # Identify trigger-related parameters
                                        if ($param.Key -match '^(trigger|window|scheduled)') {
                                            $pipelineInfo.TriggerParameters[$param.Key] = $param.Value
                                            
                                            # Also add to trigger system variables
                                            if (-not $result.TriggerSystemVariables. ContainsKey($param.Key)) {
                                                $result.TriggerSystemVariables[$param.Key] = $param. Value
                                            }
                                        }
                                    }
                                }
                                
                                # Additional run info
                                $pipelineInfo | Add-Member -NotePropertyName 'RunStart' -NotePropertyValue $pipelineRun.RunStart
                                $pipelineInfo | Add-Member -NotePropertyName 'RunEnd' -NotePropertyValue $pipelineRun.RunEnd
                                $pipelineInfo | Add-Member -NotePropertyName 'Status' -NotePropertyValue $pipelineRun.Status
                            }
                        } catch {
                            Write-Host "        Warning: Could not get pipeline run details for $($pipelineEntry.Key)" -ForegroundColor DarkYellow
                        }
                        
                        $result.TriggeredPipelines += $pipelineInfo
                    }
                }
                
                $allResults += $result
            }
        } else {
            Write-Host "    No runs found" -ForegroundColor Gray
        }
        
    } catch {
        Write-Warning "Failed to get runs for trigger '$($trigger.Name)': $($_.Exception.Message)"
    }
}

# Display results
if ($allResults.Count -gt 0) {
    Write-Host "`n==================== FOUND $($allResults.Count) TRIGGER RUN(S) ====================" -ForegroundColor Green
    
    # Display summary table
    Write-Host "`n==================== SUMMARY ====================" -ForegroundColor Cyan
    $allResults | Select-Object `
        TriggerName, `
        Status, `
        @{N='Timestamp';E={$_.TriggerRunTimestamp. ToString('yyyy-MM-dd HH:mm:ss')}}, `
        @{N='Pipelines';E={($_.TriggeredPipelines. PipelineName) -join ', '}}, `
        TriggerRunId | Format-Table -AutoSize
    
    # Display detailed information
    Write-Host "`n==================== DETAILED PARAMETERS ====================" -ForegroundColor Cyan
    
    foreach ($result in $allResults) {
        Write-Host "`n╔═══════════════════════════════════════════════════════════════" -ForegroundColor Yellow
        Write-Host "║ Trigger: $($result.TriggerName)" -ForegroundColor Yellow
        Write-Host "╠═══════════════════════════════════════════════════════════════" -ForegroundColor Yellow
        Write-Host "║ Status: $($result.Status)" -ForegroundColor $(if($result.Status -eq 'Succeeded'){'Green'}else{'Red'})
        Write-Host "║ Run ID: $($result.TriggerRunId)"
        Write-Host "║ Type: $($result.TriggerType)"
        Write-Host "║ Timestamp: $($result.TriggerRunTimestamp. ToString('yyyy-MM-dd HH:mm:ss'))"
        
        # Trigger Definition Parameters
        Write-Host "║" -ForegroundColor Yellow
        Write-Host "║ ┌─ TRIGGER DEFINITION PARAMETERS (from trigger config):" -ForegroundColor Cyan
        if ($result.TriggerDefinitionParameters -and $result.TriggerDefinitionParameters.Count -gt 0) {
            $result.TriggerDefinitionParameters.GetEnumerator() | ForEach-Object {
                $value = if ($_.Value -is [string]) { $_.Value } else { $_.Value | ConvertTo-Json -Compress }
                Write-Host "║ │  • $($_.Key): $value" -ForegroundColor White
            }
        } else {
            Write-Host "║ │  (no parameters defined in trigger)" -ForegroundColor Gray
        }
        Write-Host "║ └─"
        
        # Trigger System Variables
        Write-Host "║" -ForegroundColor Yellow
        Write-Host "║ ┌─ TRIGGER SYSTEM VARIABLES (runtime values):" -ForegroundColor Cyan
        if ($result.TriggerSystemVariables -and $result.TriggerSystemVariables.Count -gt 0) {
            $result.TriggerSystemVariables.GetEnumerator() | Sort-Object Key | ForEach-Object {
                $value = if ($_.Value -is [string]) { $_.Value } else { $_.Value | ConvertTo-Json -Compress }
                Write-Host "║ │  • $($_.Key): $value" -ForegroundColor White
            }
        } else {
            Write-Host "║ │  (no system variables captured)" -ForegroundColor Gray
        }
        Write-Host "║ └─"
        
        # Trigger Run Properties
        Write-Host "║" -ForegroundColor Yellow
        Write-Host "║ ┌─ TRIGGER RUN PROPERTIES:" -ForegroundColor Cyan
        if ($result.TriggerRunProperties -and $result.TriggerRunProperties.Count -gt 0) {
            $result.TriggerRunProperties.GetEnumerator() | ForEach-Object {
                $value = if ($_. Value -is [string]) { $_.Value } else { $_.Value | ConvertTo-Json -Compress }
                Write-Host "║ │  • $($_. Key): $value" -ForegroundColor White
            }
        } else {
            Write-Host "║ │  (no additional properties)" -ForegroundColor Gray
        }
        Write-Host "║ └─"
        
        # Triggered Pipelines
        if ($result.TriggeredPipelines. Count -gt 0) {
            Write-Host "║" -ForegroundColor Yellow
            Write-Host "║ ┌─ TRIGGERED PIPELINES:" -ForegroundColor Cyan
            
            foreach ($pipeline in $result.TriggeredPipelines) {
                Write-Host "║ │" -ForegroundColor Yellow
                Write-Host "║ │  ╔══► Pipeline: $($pipeline.PipelineName)" -ForegroundColor Magenta
                Write-Host "║ │  ║   Run ID: $($pipeline.RunId)"
                Write-Host "║ │  ║   Status: $($pipeline.Status)"
                if ($pipeline.RunStart) {
                    Write-Host "║ │  ║   Started: $($pipeline.RunStart. ToString('yyyy-MM-dd HH:mm:ss'))"
                }
                
                # Trigger Parameters passed to pipeline
                if ($pipeline.TriggerParameters -and $pipeline.TriggerParameters.Count -gt 0) {
                    Write-Host "║ │  ║"
                    Write-Host "║ │  ║   ┌─ Trigger Parameters (passed to this pipeline):" -ForegroundColor Green
                    $pipeline.TriggerParameters.GetEnumerator() | Sort-Object Key | ForEach-Object {
                        $value = if ($_.Value -is [string]) { $_.Value } else { $_.Value | ConvertTo-Json -Compress }
                        Write-Host "║ │  ║   │  • $($_.Key): $value" -ForegroundColor White
                    }
                    Write-Host "║ │  ║   └─"
                }
                
                # All Parameters
                if ($pipeline.Parameters -and $pipeline.Parameters.Count -gt 0) {
                    Write-Host "║ │  ║"
                    Write-Host "║ │  ║   ┌─ All Pipeline Parameters:" -ForegroundColor Yellow
                    $pipeline.Parameters. GetEnumerator() | Sort-Object Key | ForEach-Object {
                        $value = if ($_.Value -is [string]) { $_.Value } else { $_.Value | ConvertTo-Json -Compress }
                        
                        # Highlight trigger-related params
                        if ($_.Key -match '^(trigger|window|scheduled)') {
                            Write-Host "║ │  ║   │  • $($_. Key): $value" -ForegroundColor Cyan
                        } else {
                            Write-Host "║ │  ║   │  • $($_.Key): $value" -ForegroundColor Gray
                        }
                    }
                    Write-Host "║ │  ║   └─"
                } else {
                    Write-Host "║ │  ║   Parameters: (none)"
                }
                
                Write-Host "║ │  ╚═══"
            }
            Write-Host "║ └─"
        }
        
        # Message
        if ($result.Message) {
            Write-Host "║" -ForegroundColor Yellow
            Write-Host "║ Message: $($result.Message)" -ForegroundColor Gray
        }
        
        Write-Host "╚═══════════════════════════════════════════════════════════════" -ForegroundColor Yellow
    }
    
    # Export to JSON
    if ($ExportJson) {
        $jsonExport = $allResults | ForEach-Object {
            [PSCustomObject]@{
                TriggerName = $_.TriggerName
                TriggerRunId = $_.TriggerRunId
                TriggerType = $_.TriggerType
                Status = $_.Status
                TriggerRunTimestamp = $_.TriggerRunTimestamp. ToString('o')
                Message = $_.Message
                TriggerDefinitionParameters = $_.TriggerDefinitionParameters
                TriggerSystemVariables = $_.TriggerSystemVariables
                TriggerRunProperties = $_.TriggerRunProperties
                TriggeredPipelines = $_. TriggeredPipelines | ForEach-Object {
                    @{
                        PipelineName = $_.PipelineName
                        RunId = $_.RunId
                        Status = $_.Status
                        RunStart = if ($_.RunStart) { $_.RunStart.ToString('o') } else { $null }
                        RunEnd = if ($_.RunEnd) { $_.RunEnd.ToString('o') } else { $null }
                        AllParameters = $_.Parameters
                        TriggerParameters = $_.TriggerParameters
                    }
                }
            }
        }
        
        $jsonExport | ConvertTo-Json -Depth 10 | Out-File -FilePath $ExportJson -Encoding UTF8
        Write-Host "`n✓ Full details exported to JSON:  $ExportJson" -ForegroundColor Green
    }
    
    # Export to CSV
    if ($ExportCsv) {
        $csvExport = $allResults | ForEach-Object {
            $item = $_
            [PSCustomObject]@{
                TriggerName = $item.TriggerName
                TriggerRunId = $item. TriggerRunId
                TriggerType = $item.TriggerType
                Status = $item.Status
                Timestamp = $item.TriggerRunTimestamp.ToString('yyyy-MM-dd HH:mm: ss')
                TriggerDefinitionParams = if ($item.TriggerDefinitionParameters. Count -gt 0) {
                    ($item.TriggerDefinitionParameters.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join "; "
                } else { "" }
                TriggerSystemVariables = if ($item.TriggerSystemVariables.Count -gt 0) {
                    ($item.TriggerSystemVariables.GetEnumerator() | ForEach-Object { "$($_. Key)=$($_.Value)" }) -join "; "
                } else { "" }
                TriggerRunProperties = if ($item. TriggerRunProperties.Count -gt 0) {
                    ($item.TriggerRunProperties.GetEnumerator() | ForEach-Object { "$($_. Key)=$($_.Value)" }) -join "; "
                } else { "" }
                TriggeredPipelines = ($item.TriggeredPipelines.PipelineName) -join ", "
                PipelineRunIds = ($item.TriggeredPipelines.RunId) -join ", "
                PipelineParameters = ($item.TriggeredPipelines | ForEach-Object {
                    if ($_.Parameters.Count -gt 0) {
                        $params = ($_.Parameters.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join ", "
                        "$($_.PipelineName): {$params}"
                    }
                }) -join " | "
                Message = $item.Message
            }
        }
        
        $csvExport | Export-Csv -Path $ExportCsv -NoTypeInformation -Encoding UTF8
        Write-Host "✓ Summary exported to CSV: $ExportCsv" -ForegroundColor Green
    }
    
    return $allResults
    
} else {
    Write-Host "`n╔════════════════════════════════════════════╗" -ForegroundColor Yellow
    Write-Host "║ No trigger runs found                      ║" -ForegroundColor Yellow
    Write-Host "╚════════════════════════════════════════════╝" -ForegroundColor Yellow
    Write-Host "`nTroubleshooting:" -ForegroundColor Gray
    Write-Host "  • Try increasing -DaysBack parameter (currently:  $DaysBack)" -ForegroundColor Gray
    Write-Host "  • Check if triggers have been executed recently in ADF portal" -ForegroundColor Gray
    Write-Host "  • Verify trigger names are correct" -ForegroundColor Gray
}

