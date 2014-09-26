namespace Problem

open System

module MonaLisa =

    type Point =
        struct
            val X:float
            val Y:float
            new (x:float,y:float) = { X=x; Y=y }
        end

    type Solution = Point []

    type Evaluation = 
        struct
            val Value:float
            val Solution:Solution
            new (value:float,solution:Solution) = { Value=value; Solution=solution }
        end

    type Config = 
        {   ProbaConvince:float; 
            ProbaMistake:float;
            ProbaScout:float;
            LocalIterations:int;
            HiveSize:int; }

    let DefaultConfig = 
        {   ProbaConvince = 0.2; 
            ProbaMistake = 0.01;
            ProbaScout = 0.10;
            LocalIterations = 500000;
            HiveSize = 100; }

    let shuffle (rng:Random) xs =
        let len = xs |> Array.length
        for i in (len - 1) .. -1 .. 1 do
            let j = rng.Next(i + 1)
            let temp = xs.[j]
            xs.[j] <- xs.[i]
            xs.[i] <- temp
        xs

    let dist (p1:Point) (p2:Point) = 
        (p1.X - p2.X) * (p1.X - p2.X) + (p1.Y - p2.Y) * (p1.Y - p2.Y)

    let length (xs:Solution) =
        let len = xs |> Array.length
        xs 
        |> Seq.fold (fun (acc,prev) x -> 
            acc + dist x xs.[prev], (prev+1)%len) (0.,len-1) 
        |> fst

    let quality (xs:Solution) = - length xs

    let localSearch (rng:Random) (xs:Solution) =
        let len = xs |> Array.length
        let iters = DefaultConfig.LocalIterations
        let switch (xs:'a[]) i j = 
            let temp = xs.[i]
            xs.[i] <- xs.[j]
            xs.[j] <- temp
            xs
        let safeLo i = if i >= 0 then i else (len-1)
        let safeHi i = if i < (len-1) then i else 0

        let rec search (xs:Solution) iter =
            if iter > iters
            then xs
            else
                let i,j = rng.Next(len),rng.Next(len)
                let before = 
                    dist xs.[safeLo(i-1)] xs.[i] 
                    + dist xs.[i] xs.[safeHi(i+1)]
                    + dist xs.[safeLo(j-1)] xs.[j] 
                    + dist xs.[j] xs.[safeHi(j+1)]
                let after = 
                    dist xs.[safeLo(i-1)] xs.[j] 
                    + dist xs.[j] xs.[safeHi(i+1)]
                    + dist xs.[safeLo(j-1)] xs.[i] 
                    + dist xs.[i] xs.[safeHi(j+1)]

                let improvement = after < before
                let mistake = rng.NextDouble () < DefaultConfig.ProbaMistake
                let next = 
                    match (improvement,mistake) with
                    | true, false -> switch xs i j
                    | true, true -> xs
                    | false, false -> xs
                    | false, true -> switch xs i j
                search next (iter + 1)
        search xs 0

    type Estimates = Map<int,float> // for various load levels, throughput

    let alpha = 0.2
    let epsilon = 0.5

    let learn (current:Estimates) (experience:int*float) =
        let load,time = experience
        match current.TryFind load with
        | Some(estimate) -> current.Add (load, (1.-alpha) * estimate + alpha * time)
        | None -> current.Add (load, alpha * time)
    
    let decide (rng:Random) (current:Estimates) (load:int) =
        let alternatives =
            match load with
            | 1 -> [| 1; 2; |]
            | x -> [| x - 1; x; x + 1 |]
        let p = rng.NextDouble ()
        if (p < epsilon)
        then alternatives.[rng.Next(alternatives.Length)]            
        else
            alternatives
            |> Seq.map (fun l -> 
                l, 
                match current.TryFind l with
                | Some(v) -> v
                | None -> Double.MaxValue)
            |> Seq.minBy snd
            |> fst