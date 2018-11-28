namespace SharpFunky

module Validation =

    module NoErrors =
        open System.Text.RegularExpressions

        let conditional fn : TrialFn<_, _, _> = fun x -> if fn x then Trial.success() else Trial.failure()

        let shouldNotBeNull<'a when 'a: null> : TrialFn<'a, _, _> =
          conditional (isNull >> not)

        let shouldNotBeEmpty =
          conditional String.isNotNullOrEmpty
        let shouldNotBeBlank =
          conditional String.isNotNullOrWS

        let shouldBeLongerThan length =
          conditional (fun s -> String.length s > length)
        let shouldNotBeLongerThan length =
          conditional (fun s -> String.length s <= length)
        let shouldBeShorterThan length =
          conditional (fun s -> String.length s < length)
        let shouldNotBeShorterThan length =
          conditional (fun s -> String.length s >= length)
        let shouldHaveAtLeast length =
          conditional (fun s -> String.length s >= length)
        let shouldHaveAtMost length =
          conditional (fun s -> String.length s <= length)
        let lengthShouldBeBetween min max =
          conditional (fun s -> String.length s |> fun l -> min <= l && l <= max)

        let shouldMatchPatternWithOptions options pattern =
            let regex = Regex(pattern, options)
            conditional (fun s -> isNotNull s && regex.IsMatch(s))
        let shouldMatchPattern = shouldMatchPatternWithOptions RegexOptions.Compiled

        let shouldBeGreaterThan a =
          conditional (fun v -> v > a)
        let shouldNotBeGreaterThan a =
          conditional (fun v -> v <= a)
        let shouldBeGreaterThanOrEqualTo a =
          conditional (fun v -> v >= a)
        let shouldNotBeGreaterThanOrEqualTo a =
          conditional (fun v -> v < a)
        let shouldBeLessThan a =
          conditional (fun v -> v < a)
        let shouldNotBeLessThan a =
          conditional (fun v -> v >= a)
        let shouldBeLessThanOrEqualTo a =
          conditional (fun v -> v <= a)
        let shouldNotBeLessThanOrEqualTo a =
          conditional (fun v -> v > a)
        let shouldBeEqualTo a =
          conditional (fun v -> v = a)
        let shouldNotBeEqualTo a =
          conditional (fun v -> v <> a)
        let shouldBeBetween min max =
          conditional (fun v -> min <= v && v <= max)
        let shouldNotBeBetween min max =
          conditional (fun v -> (min <= v && v <= max) |> not)
        let shouldBeExclusivelyBetween min max =
          conditional (fun v -> min < v && v < max)
        let shouldNotBeExclusivelyBetween min max =
          conditional (fun v -> (min < v && v < max) |> not)

    let internal resources = lazy ResX.fromThis "Resources.Validation"

    let withFailure errFn condFn =
        condFn |> TrialFn.mapMessages (fun _ -> [errFn()])
    let withResFailure resName =
        withFailure (fun () -> resources.Value.getString(resName))
    let withResParamFailure resName (ps: _[]) =
        withFailure (fun () -> System.String.Format(resources.Value.getString(resName), ps))

    let shouldNotBeNull = fun x ->
        x |> (NoErrors.shouldNotBeNull
              |> withResFailure "ShouldNotBeNull")

    let shouldNotBeEmpty =
        NoErrors.shouldNotBeEmpty
        |> withResFailure "ShouldNotBeEmpty"
    let shouldNotBeBlank =
        NoErrors.shouldNotBeBlank
        |> withResFailure "ShouldNotBeBlank"

    let shouldBeLongerThan length =
        NoErrors.shouldBeLongerThan length
        |> withResParamFailure "ShouldBeLongerThan" [| length |]
    let shouldNotBeLongerThan length =
        NoErrors.shouldNotBeLongerThan length
        |> withResParamFailure "ShouldNotBeLongerThan" [| length |]
    let shouldBeShorterThan length =
        NoErrors.shouldBeShorterThan length
        |> withResParamFailure "ShouldBeShorterThan" [| length |]
    let shouldNotBeShorterThan length =
        NoErrors.shouldNotBeShorterThan length
        |> withResParamFailure "ShouldNotBeShorterThan" [| length |]
    let shouldHaveAtLeast length =
        NoErrors.shouldHaveAtLeast length
        |> withResParamFailure "ShouldHaveAtLeast" [| length |]
    let shouldHaveAtMost length =
        NoErrors.shouldHaveAtMost length
        |> withResParamFailure "ShouldHaveAtMost" [| length |]
    let lengthShouldBeBetween min max =
        NoErrors.lengthShouldBeBetween min max
        |> withResParamFailure "LengthShouldBeBetween" [| min; max |]
    let shouldMatchPatternWithOptions options pattern errFn =
        NoErrors.shouldMatchPatternWithOptions options pattern
        |> withFailure errFn
    let shouldMatchPattern pattern errFn =
        NoErrors.shouldMatchPattern pattern
        |> withFailure errFn

    let shouldBeGreaterThan a =
        NoErrors.shouldBeGreaterThan a
        |> withResParamFailure "ShouldBeGreaterThan" [| a |]
    let shouldNotBeGreaterThan a =
        NoErrors.shouldNotBeGreaterThan a
        |> withResParamFailure "ShouldNotBeGreaterThan" [| a |]
    let shouldBeGreaterThanOrEqualTo a =
        NoErrors.shouldBeGreaterThanOrEqualTo a
        |> withResParamFailure "ShouldBeGreaterThanOrEqualTo" [| a |]
    let shouldNotBeGreaterThanOrEqualTo a =
        NoErrors.shouldNotBeGreaterThanOrEqualTo a
        |> withResParamFailure "ShouldNotBeGreaterThanOrEqualTo" [| a |]
    let shouldBeLessThan a =
        NoErrors.shouldBeLessThan a
        |> withResParamFailure "ShouldBeLessThan" [| a |]
    let shouldNotBeLessThan a =
        NoErrors.shouldNotBeLessThan a
        |> withResParamFailure "ShouldNotBeLessThan" [| a |]
    let shouldBeLessThanOrEqualTo a =
        NoErrors.shouldBeLessThanOrEqualTo a
        |> withResParamFailure "ShouldBeLessThanOrEqualTo" [| a |]
    let shouldNotBeLessThanOrEqualTo a =
        NoErrors.shouldNotBeLessThanOrEqualTo a
        |> withResParamFailure "ShouldNotBeLessThanOrEqualTo" [| a |]
    let shouldBeEqualTo a =
        NoErrors.shouldBeEqualTo a
        |> withResParamFailure "ShouldBeEqualTo" [| a |]
    let shouldNotBeEqualTo a =
        NoErrors.shouldNotBeEqualTo a
        |> withResParamFailure "ShouldNotBeEqualTo" [| a |]
    let shouldBeBetween min max =
        NoErrors.shouldBeBetween min max
        |> withResParamFailure "ShouldBeBetween" [| min; max |]
    let shouldNotBeBetween min max =
        NoErrors.shouldNotBeBetween min max
        |> withResParamFailure "ShouldNotBeBetween" [| min; max |]
    let shouldBeExclusivelyBetween min max =
        NoErrors.shouldBeExclusivelyBetween min max
        |> withResParamFailure "ShouldBeExclusivelyBetween" [| min; max |]
    let shouldNotBeExclusivelyBetween min max =
        NoErrors.shouldNotBeExclusivelyBetween min max
        |> withResParamFailure "ShouldNotBeExclusivelyBetween" [| min; max |]
