#addin "Nuget.Core"
#addin "Cake.ExtendedNuGet"

var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

var sln = new FilePath("./CSharpDriver.sln");

Task("BuildNet45")
    .Does(() =>
    {
        NuGetRestore(sln);
        DotNetBuild(sln, settings => settings
            .SetConfiguration(configuration)
            .SetVerbosity(Verbosity.Minimal));
    });

Task("BuildNetStandard15")
    .Does(() =>
    {
        DotNetCoreRestore();
        var buildSettings = new DotNetCoreBuildSettings
        {
            Configuration = configuration
        };
        DotNetCoreBuild("./**/project.json", buildSettings);
    });

Task("Build")
    .IsDependentOn("BuildNet45")
    .IsDependentOn("BuildNetStandard15");

Task("Default")
    .IsDependentOn("BuildNet45");

RunTarget(target);