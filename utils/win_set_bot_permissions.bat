@echo off
chcp 65001 > nul
setlocal EnableDelayedExpansion

REM === Initial Info ===
echo ========================================================================
echo == IMPORTANT: This script must be run as ADMINISTRATOR!             ==
echo == To do this, right-click on this file and select                  ==
echo == "Run as administrator".                                          ==
echo ========================================================================
echo.
echo This script will check and adjust file permissions for your bot.
echo It targets *.json, *.txt, *.env, and *.env.gpg files, ensuring
echo only you (the current user), SYSTEM, and Administrators
 echo have appropriate access.
echo.
echo Press any key to continue with permission checks,
echo or close this window to cancel...
pause > nul

REM === Setup ===
set "BASE_PATH=%~dp0"
set "FILES_CHECKED=0"
set "FILES_MODIFIED=0"
set "FILES_UNCHANGED=0"

set "TEMP_ACL_BEFORE=%TEMP%\bot_acl_before.txt"
set "TEMP_ACL_AFTER=%TEMP%\bot_acl_after.txt"

set "SID_ADMINS=*S-1-5-32-544"
set "SID_EVERYONE=*S-1-1-0"
set "SID_USERS=*S-1-5-32-545"
set "SID_AUTH_USERS=*S-1-5-11"
set "SID_SYSTEM=*S-1-5-18"

REM === Main Processing ===
for %%F in ("%BASE_PATH%*.json" "%BASE_PATH%*.txt" "%BASE_PATH%*.env" "%BASE_PATH%*.env.gpg") do (
    set /a FILES_CHECKED+=1
    set "CURRENT_FILE=%%~fF"
    echo Processing: %%~nxF

    icacls "%%F" /save "%TEMP_ACL_BEFORE%" > nul 2>&1
    icacls "%%F" /inheritance:r > nul 2>&1
    icacls "%%F" /grant "%USERNAME%:(M)" > nul 2>&1
    icacls "%%F" /grant "%SID_SYSTEM%:(F)" > nul 2>&1
    icacls "%%F" /grant "%SID_ADMINS%:(F)" > nul 2>&1
    icacls "%%F" /remove "%SID_EVERYONE%" > nul 2>&1
    icacls "%%F" /remove "%SID_USERS%" > nul 2>&1
    icacls "%%F" /remove "%SID_AUTH_USERS%" > nul 2>&1
    icacls "%%F" /save "%TEMP_ACL_AFTER%" > nul 2>&1

    fc /A "%TEMP_ACL_BEFORE%" "%TEMP_ACL_AFTER%" > nul 2>&1
    if !errorlevel! equ 0 (
        echo   No changes needed.
        set /a FILES_UNCHANGED+=1
    ) else (
        echo   Permissions updated.
        set /a FILES_MODIFIED+=1
    )

    del "%TEMP_ACL_BEFORE%" 2>nul
    del "%TEMP_ACL_AFTER%" 2>nul
)

REM === Summary ===
echo.
echo ===============================================================
echo Summary:
echo   Total files checked:     %FILES_CHECKED%
echo   Files updated:           %FILES_MODIFIED%
echo   Files already correct:   %FILES_UNCHANGED%
echo ===============================================================

REM === Final cleanup and pause ===
echo.
echo Script complete. Press any key to close.
pause
endlocal
